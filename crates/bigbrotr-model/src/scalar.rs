use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{fmt, marker::PhantomData, num::NonZeroU64, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarError {
    Zero,
    Exhausted,
}

impl fmt::Display for ScalarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Zero => "allocated counters start at one",
            Self::Exhausted => {
                "counter exhausted; explicit allocation or format transition required"
            }
        })
    }
}
impl std::error::Error for ScalarError {}

// Metadata uses decimal strings. Protocol NostrTime deliberately has its own
// numeric JSON mapping; it must never pass through this adapter.
pub(crate) fn decimal<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: FromStr + fmt::Display,
    T::Err: fmt::Display,
    D: Deserializer<'de>,
{
    struct Decimal<T>(PhantomData<T>);
    impl<T> de::Visitor<'_> for Decimal<T>
    where
        T: FromStr + fmt::Display,
        T::Err: fmt::Display,
    {
        type Value = T;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a canonical decimal integer string")
        }
        fn visit_str<E: de::Error>(self, value: &str) -> Result<T, E> {
            let parsed: T = value.parse().map_err(E::custom)?;
            if parsed.to_string() != value {
                return Err(E::custom("noncanonical decimal integer"));
            }
            Ok(parsed)
        }
    }
    deserializer.deserialize_str(Decimal(PhantomData))
}

macro_rules! nonzero_counter {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(NonZeroU64);
        impl $name {
            pub fn new(value: u64) -> Result<Self, ScalarError> {
                NonZeroU64::new(value).map(Self).ok_or(ScalarError::Zero)
            }
            pub const fn get(self) -> u64 {
                self.0.get()
            }
            pub fn checked_add(self, amount: u64) -> Result<Self, ScalarError> {
                Self::new(
                    self.get()
                        .checked_add(amount)
                        .ok_or(ScalarError::Exhausted)?,
                )
            }
        }
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(&self.get())
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Self::new(decimal(deserializer)?).map_err(de::Error::custom)
            }
        }
    };
}

nonzero_counter!(Ordinal);
nonzero_counter!(OwnerEpoch);
nonzero_counter!(Sequence);
nonzero_counter!(ProducerEpoch);
nonzero_counter!(RequestSequence);

macro_rules! measured_scalar {
    ($name:ident, $value:ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name($value);
        impl $name {
            pub const fn new(value: $value) -> Self {
                Self(value)
            }
            pub const fn get(self) -> $value {
                self.0
            }
        }
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(&self.0)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                decimal(deserializer).map(Self)
            }
        }
    };
}

measured_scalar!(ObservedTime, i64);
measured_scalar!(DurationUs, u64);
measured_scalar!(CompleteThrough, u64);
measured_scalar!(TopologyGeneration, u64);
measured_scalar!(PolicyEpoch, u64);

impl PolicyEpoch {
    pub fn checked_add(self, amount: u64) -> Result<Self, ScalarError> {
        self.get()
            .checked_add(amount)
            .map(Self::new)
            .ok_or(ScalarError::Exhausted)
    }
}

impl TopologyGeneration {
    pub fn checked_add(self, amount: u64) -> Result<Self, ScalarError> {
        self.get()
            .checked_add(amount)
            .map(Self::new)
            .ok_or(ScalarError::Exhausted)
    }
}
