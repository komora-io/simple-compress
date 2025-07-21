use std::panic::Location;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

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
    KnownLayout,
    Immutable,
    IntoBytes,
    Unaligned,
)]
#[repr(C)]
struct Literal {
    data: [u8; 8],
    len: u8,
}

impl AsRef<[u8]> for Literal {
    fn as_ref(&self) -> &[u8] {
        &self.data[..usize::from(self.len)]
    }
}

#[derive(Debug, Copy, Clone)]
pub struct DictionaryCreationError {
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
    literals: [Literal; 255],
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
        // TODO make real

        Dictionary {
            literals: [Literal {
                data: [0; 8],
                len: 0,
            }; 255],
        }
    }

    /// Create a dictionary from a corpus of samples.
    pub fn create_from_corpus(samples: &[&[u8]]) -> Dictionary {
        todo!()
    }

    fn position_of_literal(&self, literal: &[u8]) -> Option<u8> {
        None
    }

    fn get_literal(&self, id: u8) -> Option<&[u8]> {
        if id == u8::MAX {
            return None;
        }

        Some(self.literals[usize::from(id)].as_ref())
    }

    pub fn compress(&self, data: &[u8]) -> Vec<u8> {
        let mut compressed = vec![];

        // look for longest prefix in dictionary, use its code if exists, 255+byte otherwise

        // TODO make real

        for byte in data {
            compressed.extend_from_slice(&[u8::MAX, *byte]);
        }

        compressed
    }

    /// Given compressed data and a dictionary, return the decompressed data.
    pub fn decompress(&self, compressed_data: &[u8]) -> Vec<u8> {
        let mut decompressed = vec![];

        let mut cursor = &compressed_data[..];

        while !cursor.is_empty() {
            if let Some(literal) = self.get_literal(cursor[0]) {
                decompressed.extend_from_slice(literal);
                cursor = &cursor[1..];
            } else {
                assert_eq!(cursor[0], u8::MAX);
                decompressed.push(cursor[1]);
                cursor = &cursor[2..];
            }
        }

        decompressed
    }
}

#[test]
fn simple_fuzz_roundtrip() {
    use rand::{thread_rng, Rng};

    let mut rng = thread_rng();
    let mut buf = vec![0_u8; rng.random_range(0..1024 * 1024)];
    rng.fill(&mut buf[..]);

    let dictionary = Dictionary::create_from_sample(&buf);

    let compressed_buf = dictionary.compress(&buf);

    let decompressed_buf = dictionary.decompress(&compressed_buf);

    assert_eq!(&buf, &decompressed_buf);

    let serialized_dictionary = dictionary.as_bytes();

    let deserialized_dictionary = Dictionary::try_from_bytes(serialized_dictionary).unwrap();

    assert_eq!(&dictionary, &deserialized_dictionary);

    let decompressed_buf_2 = deserialized_dictionary.decompress(&compressed_buf);

    assert_eq!(&buf, &decompressed_buf_2);
}
