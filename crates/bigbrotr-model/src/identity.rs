use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{fmt, str::FromStr};

/// A fixed identity's wire spelling must be exactly sized, lowercase hexadecimal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidHex;

impl fmt::Display for InvalidHex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("expected exact-length lowercase hexadecimal")
    }
}
impl std::error::Error for InvalidHex {}

fn decode<const N: usize>(text: &str) -> Result<[u8; N], InvalidHex> {
    if text.len() != N * 2 {
        return Err(InvalidHex);
    }
    let nibble = |byte| match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(InvalidHex),
    };
    let mut bytes = [0; N];
    for (target, pair) in bytes.iter_mut().zip(text.as_bytes().as_chunks::<2>().0) {
        *target = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Ok(bytes)
}

macro_rules! fixed_hex {
    ($name:ident, $size:literal) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; $size]);

        impl $name {
            pub const fn from_bytes(bytes: [u8; $size]) -> Self {
                Self(bytes)
            }
            pub const fn as_bytes(&self) -> &[u8; $size] {
                &self.0
            }
        }
        impl FromStr for $name {
            type Err = InvalidHex;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                decode(value).map(Self)
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for byte in self.0 {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(self, f)
            }
        }
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct Visitor;
                impl de::Visitor<'_> for Visitor {
                    type Value = $name;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        write!(f, "{} lowercase hexadecimal characters", $size * 2)
                    }
                    fn visit_str<E: de::Error>(self, value: &str) -> Result<$name, E> {
                        value.parse().map_err(E::custom)
                    }
                }
                deserializer.deserialize_str(Visitor)
            }
        }
    };
}

fixed_hex!(EventId, 32);
fixed_hex!(PublicKey, 32);
fixed_hex!(Signature, 64);
