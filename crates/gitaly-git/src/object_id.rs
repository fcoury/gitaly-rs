//! object id and object hash primitives.

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectHash {
    Sha1,
    Sha256,
}

impl ObjectHash {
    const fn expected_len(self) -> usize {
        match self {
            Self::Sha1 => 40,
            Self::Sha256 => 64,
        }
    }
}

impl std::fmt::Display for ObjectHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sha1 => f.write_str("sha1"),
            Self::Sha256 => f.write_str("sha256"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectId(String);

impl ObjectId {
    pub fn new(value: impl Into<String>, hash: ObjectHash) -> Result<Self, ObjectIdError> {
        let value = value.into();
        let expected = hash.expected_len();
        let actual = value.len();

        if actual != expected {
            return Err(ObjectIdError::InvalidLength {
                hash,
                expected,
                actual,
            });
        }

        for (index, character) in value.chars().enumerate() {
            if !matches!(character, '0'..='9' | 'a'..='f') {
                return Err(ObjectIdError::InvalidHexCharacter { index, character });
            }
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn hash(&self) -> ObjectHash {
        match self.0.len() {
            40 => ObjectHash::Sha1,
            64 => ObjectHash::Sha256,
            _ => unreachable!("ObjectId can only be constructed with a valid hash length"),
        }
    }

    pub const fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ObjectIdError {
    #[error("invalid object id length for {hash}: expected {expected}, got {actual}")]
    InvalidLength {
        hash: ObjectHash,
        expected: usize,
        actual: usize,
    },
    #[error("invalid lowercase hex character `{character}` at index {index}")]
    InvalidHexCharacter { index: usize, character: char },
}

#[cfg(test)]
mod tests {
    use super::{ObjectHash, ObjectId, ObjectIdError};

    #[test]
    fn creates_valid_sha1_object_id() -> Result<(), ObjectIdError> {
        let oid = ObjectId::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ObjectHash::Sha1)?;

        assert_eq!(oid.as_str(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(oid.hash(), ObjectHash::Sha1);
        assert_eq!(oid.len(), 40);

        Ok(())
    }

    #[test]
    fn creates_valid_sha256_object_id() -> Result<(), ObjectIdError> {
        let oid = ObjectId::new(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ObjectHash::Sha256,
        )?;

        assert_eq!(
            oid.as_str(),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        assert_eq!(oid.hash(), ObjectHash::Sha256);
        assert_eq!(oid.len(), 64);

        Ok(())
    }

    #[test]
    fn rejects_sha1_with_invalid_length() {
        let err = ObjectId::new("aaaaaaaaaa", ObjectHash::Sha1).expect_err("must fail");

        assert!(matches!(
            err,
            ObjectIdError::InvalidLength {
                hash: ObjectHash::Sha1,
                expected: 40,
                actual: 10
            }
        ));
    }

    #[test]
    fn rejects_sha256_with_invalid_length() {
        let err = ObjectId::new("bbbbbbbbbb", ObjectHash::Sha256).expect_err("must fail");

        assert!(matches!(
            err,
            ObjectIdError::InvalidLength {
                hash: ObjectHash::Sha256,
                expected: 64,
                actual: 10
            }
        ));
    }

    #[test]
    fn rejects_uppercase_hex() {
        let err = ObjectId::new("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", ObjectHash::Sha1)
            .expect_err("must fail");

        assert!(matches!(err, ObjectIdError::InvalidHexCharacter { .. }));
    }

    #[test]
    fn rejects_non_hex_characters() {
        let err = ObjectId::new("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", ObjectHash::Sha1)
            .expect_err("must fail");

        assert!(matches!(err, ObjectIdError::InvalidHexCharacter { .. }));
    }
}
