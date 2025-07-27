use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem::MaybeUninit;
use std::panic::Location;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

const MAX_LITERAL_LEN: u8 = 4;
const DICTIONARY_LEN: u8 = 255;

#[derive(Debug, Copy, Clone)]
pub struct DictionaryCreationError {
    #[allow(unused)]
    caller: &'static Location<'static>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    FromBytes,
    Immutable,
    IntoBytes,
    Unaligned,
)]
#[repr(C)]
pub struct Dictionary {
    // literals are sorted lexicographically and can be binary searched
    literals: [[u8; MAX_LITERAL_LEN as usize]; DICTIONARY_LEN as usize],
    // TODO measure skipping wasted 4 bits by using a [u8; 128] as a [u4; 255] since max length is 8
    lengths: [u8; DICTIONARY_LEN as usize],
}

impl AsRef<[u8]> for Dictionary {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Dictionary {
    /// Parses a dictionary from its raw byte representation. Returns [`DictionaryCreationError`] if
    /// given invalid data.
    #[track_caller]
    pub fn try_from_bytes(raw_bytes: &[u8]) -> Result<Dictionary, DictionaryCreationError> {
        if let Ok(dictionary) = Dictionary::read_from_bytes(raw_bytes) {
            Ok(dictionary)
        } else {
            Err(DictionaryCreationError {
                caller: Location::caller(),
            })
        }
    }

    /// Create a dictionary from a single sample. You can just use the text you want
    /// compressed if you're unsure what to provide here.
    pub fn create_from_sample(sample: &[u8]) -> Dictionary {
        Self::create_from_corpus(&[sample])
    }

    /// Create a dictionary from a corpus of samples.
    pub fn create_from_corpus(corpus: &[&[u8]]) -> Dictionary {
        build_dictionary(corpus)
    }

    #[inline]
    const fn get_literal(&self, id: u8) -> &[u8] {
        if id == DICTIONARY_LEN {
            panic!("invalid id of DICTIONARY_LEN");
        }

        let len = self.lengths[id as usize] as usize;

        if len > MAX_LITERAL_LEN as usize {
            panic!("invalid literal len");
        }

        let literal: &[u8; MAX_LITERAL_LEN as usize] = &self.literals[id as usize];
        let slice_ptr: *const u8 = literal.as_ptr();

        unsafe { std::slice::from_raw_parts(slice_ptr, len as usize) }
    }

    fn position_of_literal(&self, literal: &[u8]) -> Option<u8> {
        use std::cmp::Ordering::*;

        // binary search below adapted from std implementation
        let mut size = DICTIONARY_LEN;
        let mut base = 0_u8;

        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            let cmp_literal = self.get_literal(mid);
            let cmp = cmp_literal.cmp(literal);

            // TODO use bool::select_unpredictable when it stabilizes
            base = if cmp == Greater { base } else { mid };

            size -= half;
        }

        // SAFETY: base is always in [0, size) because base <= mid.
        let cmp_literal = self.get_literal(base);
        let cmp = cmp_literal.cmp(literal);

        if cmp == Equal {
            Some(base)
        } else {
            None
        }
    }

    pub fn compress(&self, data: &[u8]) -> Vec<u8> {
        let before = std::time::Instant::now();

        let mut compressed = vec![];

        let mut cursor = &data[..];

        let mut match_count = 0;
        let mut match_bytes = 0;
        let mut non_match_count = 0;

        'outer: while !cursor.is_empty() {
            // look for longest prefix in dictionary, use its code if exists, 255+byte otherwise

            for prefix_len in (1..=cursor.len().min(MAX_LITERAL_LEN as usize)).rev() {
                // println!("looking for match for {:?}", &cursor[..prefix_len]);
                if let Some(position) = self.position_of_literal(&cursor[..prefix_len]) {
                    // println!("found it!");
                    match_count += 1;
                    match_bytes += prefix_len;

                    compressed.push(position);
                    cursor = &cursor[prefix_len..];

                    continue 'outer;
                }
                // println!("not found :(");
            }

            non_match_count += 1;
            compressed.extend_from_slice(&[u8::MAX, cursor[0]]);
            cursor = &cursor[1..];
        }

        // calculate megabytes per second by actually calculating bytes per microsecond
        let mbps = compressed.len() as u128 / before.elapsed().as_micros().max(1);

        println!("compress achieved throughput of {mbps} mbps, with {match_count} matches of {match_bytes} total bytes");
        println!("non-matched: {non_match_count} bytes that got doubled");

        compressed
    }

    /// Given compressed data and a dictionary, return the decompressed data.
    pub fn decompress(&self, compressed_data: &[u8]) -> Vec<u8> {
        let before = std::time::Instant::now();

        let mut decompressed = vec![];

        let mut cursor = &compressed_data[..];

        while !cursor.is_empty() {
            if cursor[0] == u8::MAX {
                decompressed.push(cursor[1]);
                cursor = &cursor[2..];
            } else {
                let literal = self.get_literal(cursor[0]);
                decompressed.extend_from_slice(literal);
                cursor = &cursor[1..];
            }
        }

        // calculate megabytes per second by actually calculating bytes per microsecond
        let mbps = decompressed.len() as u128 / before.elapsed().as_micros().max(1);

        println!("decompress achieved throughput of {mbps} mbps");

        decompressed
    }
}

fn gain(literal: &[u8]) -> usize {
    literal.len() - 1
}

fn build_dictionary(corpus: &[&[u8]]) -> Dictionary {
    let before = std::time::Instant::now();

    // literal to total gain and occurrences
    let mut gains_and_locations: HashMap<&[u8], (usize, Vec<usize>)> = HashMap::new();
    let mut locations: BTreeMap<(usize, usize), &[u8]> = BTreeMap::new();

    let corpus_iters = corpus.iter().flat_map(|s| {
        let windows = s.windows(MAX_LITERAL_LEN as usize);
        let remainder = s.len() % MAX_LITERAL_LEN as usize;

        let rest = &s[s.len() - remainder..];

        windows.chain(std::iter::once(rest))
    });

    for (start, full_window) in corpus_iters.enumerate() {
        for window_len in 1..=full_window.len().min(MAX_LITERAL_LEN as usize) {
            let window = &full_window[..window_len];
            let entry = gains_and_locations.entry(window).or_default();

            // add gain
            entry.0 += gain(window);

            // record location
            entry.1.push(start);

            let end = start + window.len();
            locations.insert((start, end), window);
        }
    }

    // re-structure the sorted set of (gain, literal)

    let mut by_gain = BTreeSet::<(usize, &[u8])>::new();

    for (literal, (gain, _location)) in &gains_and_locations {
        by_gain.insert((*gain, literal));
    }

    println!(
        "by_gain min: {:?}, max: {:?}",
        &by_gain.first(),
        &by_gain.last(),
    );

    // algorithm:
    // * always take the highest static gain literal
    // * for all overlapping literals, drop their gain, pop them out of the by_gain set, re-insert
    //   by their new gain
    //
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // * before this was implemented, and it was naively popping the last 255x, it worked at
    //   655kbps and resulted in compression of 5mbps where it caused a 6.3mb file to
    //   drop to 4.85mb for the urls dbtext file
    //   by_gain min: Some((2, [235])), max: Some((1599952, [10, 104, 116, 116, 112, 58, 47, 47]))
    //   ~~~~~~~~~~~~~
    //   find_literals achieved throughput of 0.6508069 mbps
    //   compress achieved throughput of 5 mbps, with 2335483 matches of 5069185 total bytes
    //   non-matched: 1258690 bytes that got doubled
    //   decompress achieved throughput of 472 mbps
    //   decompress achieved throughput of 518 mbps
    //   plaintext len: 6327875, compressed len: 4852863

    //dbg!(&gains_and_locations);
    //dbg!(&locations);

    let mut best_literals = BTreeSet::<&[u8]>::new();

    while best_literals.len() < (DICTIONARY_LEN as usize) && !by_gain.is_empty() {
        // dbg!(&by_gain);

        let (literal_gain, literal) = by_gain.pop_last().unwrap();
        if literal_gain == 0 {
            // there are no more non-zero gain literals, since we popped the last
            break;
        }

        best_literals.insert(literal);

        // find all literals, both longer and shorter, that intersect with this one, and
        // appropriately decrement their gain as this literal no longer counts

        let mut intersecting_literals_and_counts = HashMap::<&[u8], usize>::new();

        let locations_of_literal = &gains_and_locations[literal].1;
        for location in locations_of_literal {
            // find all intersecting literals and decrement their gains in the by_gain set
            // by popping them and re-inserting them

            let conflict_start = *location;
            let conflict_end = *location + literal.len();

            let first_possible_intersect = (*location).saturating_sub(MAX_LITERAL_LEN as usize - 1);

            for ((neighbor_start, neighbor_end), neighbor_literal) in
                locations.range(&(first_possible_intersect, 0)..&(conflict_end, usize::MAX))
            {
                if neighbor_literal == &literal {
                    continue;
                }

                if best_literals.contains(neighbor_literal) {
                    continue;
                }

                let intersects = *neighbor_end > conflict_start && *neighbor_start < conflict_end;

                if !intersects {
                    continue;
                }

                *intersecting_literals_and_counts
                    .entry(neighbor_literal)
                    .or_default() += 1;
            }
        }

        // dbg!(&intersecting_literals_and_counts);

        // now we reduce the intersecting gains appropriately
        for (intersecting_literal, intersection_count) in intersecting_literals_and_counts {
            // dbg!(intersecting_literal, intersection_count);

            let mut gain_and_locations = gains_and_locations.get_mut(intersecting_literal).unwrap();

            let old_gain: usize = gain_and_locations.0;

            // dbg!(old_gain, gain(intersecting_literal), intersection_count);

            gain_and_locations.0 = gain_and_locations
                .0
                .saturating_sub(intersection_count * gain(intersecting_literal));

            let new_gain: usize = gain_and_locations.0;

            assert!(
                by_gain.remove(&(old_gain, intersecting_literal)),
                "didn't find matching old gain for literal"
            );

            assert!(
                by_gain.insert((new_gain, intersecting_literal)),
                "incorrectly already had by_gain entry for ({new_gain}, {intersecting_literal:?})"
            );
        }
    }

    // NB important to sort the literals so that the binary search in Dictionary works as expected.

    let mut literals: [[u8; MAX_LITERAL_LEN as usize]; DICTIONARY_LEN as usize] = unsafe {
        MaybeUninit::<[[u8; MAX_LITERAL_LEN as usize]; DICTIONARY_LEN as usize]>::uninit()
            .assume_init()
    };
    let mut lengths: [u8; DICTIONARY_LEN as usize] =
        unsafe { MaybeUninit::<[u8; DICTIONARY_LEN as usize]>::uninit().assume_init() };

    for idx in (0..DICTIONARY_LEN as usize).rev() {
        let Some(literal) = best_literals.pop_last() else {
            lengths[0..=idx].fill(0);
            break;
        };

        literals[idx][..literal.len()].copy_from_slice(literal);

        lengths[idx] = u8::try_from(literal.len()).unwrap();
    }

    // calculate megabytes per second by actually calculating bytes per microsecond
    let corpus_len = corpus.iter().map(|s| s.len()).sum::<usize>();
    let mbps = corpus_len as f32 / before.elapsed().as_micros().max(1) as f32;

    println!("find_literals achieved throughput of {mbps} mbps");

    let ret = Dictionary { literals, lengths };

    for (x, y) in (0..(DICTIONARY_LEN - 1)).zip(1..DICTIONARY_LEN) {
        let l = ret.get_literal(x);
        let r = ret.get_literal(y);
        if l == r {
            assert_eq!(l, &[]);
        } else {
            assert!(
                l < r,
                "expected literal {l:?} at index {x} to be less than literal {r:?} at index {y}"
            );
        }
    }

    println!("dictionary min: {:?}", ret.get_literal(0));
    println!("dictionary max: {:?}", ret.get_literal(DICTIONARY_LEN - 1));

    ret
}

#[cfg(test)]
mod test {
    use rand::{rng, Rng};

    use super::Dictionary;

    fn round_trip(buf: &[u8]) {
        let dictionary = Dictionary::create_from_sample(&buf);

        let compressed_buf = dictionary.compress(&buf);

        let decompressed_buf = dictionary.decompress(&compressed_buf);

        assert_eq!(&buf, &decompressed_buf);

        let serialized_dictionary = dictionary.as_ref();

        let deserialized_dictionary = Dictionary::try_from_bytes(serialized_dictionary).unwrap();

        assert_eq!(&dictionary, &deserialized_dictionary);

        let decompressed_buf_2 = deserialized_dictionary.decompress(&compressed_buf);

        assert_eq!(&buf, &decompressed_buf_2);

        println!(
            "plaintext len: {}, compressed len: {}",
            buf.len(),
            compressed_buf.len()
        );
    }

    #[test]
    fn tiny() {
        round_trip(b"aaa");
    }

    #[test]
    fn simple_fuzz_roundtrip() {
        let mut rng = rng();

        for _ in 0..128 {
            let mut buf = vec![0_u8; rng.random_range(0..1024 * 1024)];
            rng.fill(&mut buf[..]);
            round_trip(&buf)
        }
    }

    #[test]
    fn corpus() {
        let buf = std::fs::read("dbtext/urls").unwrap();
        round_trip(&buf);
    }

    #[test]
    fn small_sizes() {
        let mut rng = rng();

        for len in 0..128 {
            let mut buf = vec![0_u8; len];
            rng.fill(&mut buf[..]);
            round_trip(&buf)
        }
    }
}
