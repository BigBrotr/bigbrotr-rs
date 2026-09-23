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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityError {
    Hex(InvalidHex),
    Zero,
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hex(error) => error.fmt(f),
            Self::Zero => f.write_str("allocated identities cannot be all zero"),
        }
    }
}
impl std::error::Error for IdentityError {}

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
        fixed_hex!(@shared $name, $size);
        impl $name {
            pub const fn from_bytes(bytes: [u8; $size]) -> Self {
                Self(bytes)
            }
        }
        impl FromStr for $name {
            type Err = InvalidHex;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                decode(value).map(Self)
            }
        }
    };
    ($name:ident, $size:literal, nonzero) => {
        fixed_hex!(@shared $name, $size);
        impl TryFrom<[u8; $size]> for $name {
            type Error = IdentityError;
            fn try_from(bytes: [u8; $size]) -> Result<Self, Self::Error> {
                if bytes == [0; $size] {
                    Err(IdentityError::Zero)
                } else {
                    Ok(Self(bytes))
                }
            }
        }
        impl FromStr for $name {
            type Err = IdentityError;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::try_from(decode(value).map_err(IdentityError::Hex)?)
            }
        }
    };
    (@shared $name:ident, $size:literal) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; $size]);

        impl $name {
            pub const fn as_bytes(&self) -> &[u8; $size] {
                &self.0
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
fixed_hex!(Digest256, 32);
fixed_hex!(DatasetId, 16, nonzero);
// Attribution identity only; parsing never authenticates a principal.
fixed_hex!(PrincipalId, 16, nonzero);
// Paired with an authenticated actor in the durable management command ledger.
// This public correlation key is neither a bearer secret nor command authority.
fixed_hex!(CommandKey, 16, nonzero);
// An approved output namespace, never a data or publication grant.
fixed_hex!(DestinationId, 16, nonzero);
// A same-session proof reference, never a bearer credential.
fixed_hex!(ReauthenticationId, 16, nonzero);
// Names control.owner_epochs.resource_id and its synchronized ingest manifest.
// A resource name is not an ownership grant.
fixed_hex!(OwnerResourceId, 16, nonzero);
fixed_hex!(AllocationDomainId, 16, nonzero);
fixed_hex!(ProducerSlotId, 16, nonzero);
// Portable identity only; a parsed nonce never establishes live process authority.
fixed_hex!(ProcessIncarnation, 16, nonzero);
fixed_hex!(ProjectionGenerationId, 16, nonzero);
// The proof format carries all bytes16 values; registration/freshness belongs
// to the recovery owner, not to this portable representation.
fixed_hex!(RecoveryGenerationId, 16);
