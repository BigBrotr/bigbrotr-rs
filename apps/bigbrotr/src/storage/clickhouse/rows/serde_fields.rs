//! Exact adapters for RowBinary's FixedString64 and nullable semantic tuples.
pub(super) mod signature {
    use serde::{Deserializer, Serializer, de, ser::SerializeTuple};
    pub fn serialize<S: Serializer>(value: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(64)?;
        for byte in value {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 64], D::Error> {
        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = [u8; 64];
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("exactly 64 signature bytes")
            }
            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut value = [0; 64];
                for (position, byte) in value.iter_mut().enumerate() {
                    *byte = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(position, &self))?;
                }
                if seq.next_element::<u8>()?.is_some() {
                    return Err(de::Error::invalid_length(65, &self));
                }
                Ok(value)
            }
        }
        deserializer.deserialize_tuple(64, Visitor)
    }
}

pub(super) mod optional_tuple {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser::SerializeSeq};
    use std::marker::PhantomData;
    pub fn serialize<T: Serialize, S: Serializer>(
        value: &Option<T>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(usize::from(value.is_some())))?;
        if let Some(value) = value {
            seq.serialize_element(value)?;
        }
        seq.end()
    }
    pub fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<T>, D::Error> {
        struct Visitor<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> de::Visitor<'de> for Visitor<T> {
            type Value = Option<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("zero or one complete qualified tuple")
            }
            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                if seq.size_hint().is_some_and(|len| len > 1) {
                    return Err(de::Error::custom(
                        "optional tuple contains more than one value",
                    ));
                }
                let value = seq.next_element()?;
                if seq.next_element::<T>()?.is_some() {
                    return Err(de::Error::custom(
                        "optional tuple contains more than one value",
                    ));
                }
                Ok(value)
            }
        }
        deserializer.deserialize_seq(Visitor(PhantomData))
    }
}

pub(super) mod tags {
    use super::super::{MAX_CORE_FACT_BYTES, MAX_TAG_READBACK_ALLOCATION_BYTES};
    use serde::{
        Deserializer, Serialize, Serializer,
        de::{self, DeserializeSeed, SeqAccess, Visitor},
    };
    use std::{borrow::Cow, mem::size_of};

    pub fn serialize<S: Serializer>(
        value: &[Vec<String>],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.serialize(serializer)
    }

    #[derive(Default)]
    struct Budget {
        nodes: usize,
        text: usize,
        allocation: usize,
    }
    impl Budget {
        fn nodes<E: de::Error>(&mut self, count: usize) -> Result<(), E> {
            self.nodes = self
                .nodes
                .checked_add(count)
                .filter(|n| *n <= MAX_CORE_FACT_BYTES)
                .ok_or_else(|| E::custom("serving tags exceed the core fact node bound"))?;
            Ok(())
        }
        fn allocation<E: de::Error>(&mut self, bytes: usize) -> Result<(), E> {
            self.allocation = self
                .allocation
                .checked_add(bytes)
                .filter(|n| *n <= MAX_TAG_READBACK_ALLOCATION_BYTES)
                .ok_or_else(|| E::custom("serving tags exceed the decoded allocation bound"))?;
            Ok(())
        }
        fn text<E: de::Error>(&mut self, bytes: usize) -> Result<(), E> {
            self.text = self
                .text
                .checked_add(bytes)
                .filter(|n| *n <= MAX_CORE_FACT_BYTES)
                .ok_or_else(|| E::custom("serving tags exceed the core fact text bound"))?;
            Ok(())
        }
        fn reserve<T, E: de::Error>(&mut self, values: &mut Vec<T>, count: usize) -> Result<(), E> {
            self.allocation(
                count
                    .checked_mul(size_of::<T>())
                    .ok_or_else(|| E::custom("tag allocation overflow"))?,
            )?;
            values
                .try_reserve_exact(count)
                .map_err(|_| E::custom("tag allocation failed"))
        }
    }

    struct Text<'a>(&'a mut Budget);
    impl<'de> DeserializeSeed<'de> for Text<'_> {
        type Value = String;
        fn deserialize<D: Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> Result<Self::Value, D::Error> {
            deserializer.deserialize_str(self)
        }
    }
    impl Visitor<'_> for Text<'_> {
        type Value = String;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("bounded exact tag text")
        }
        fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
            self.0.text(value.len())?;
            self.0.allocation(value.len())?;
            let mut owned = String::new();
            owned
                .try_reserve_exact(value.len())
                .map_err(|_| E::custom("tag text allocation failed"))?;
            owned.push_str(value);
            Ok(owned)
        }
        fn visit_string<E: de::Error>(self, value: String) -> Result<String, E> {
            self.0.text(value.len())?;
            self.0.allocation(value.capacity())?;
            Ok(value)
        }
    }

    struct Tag<'a>(&'a mut Budget);
    impl<'de> DeserializeSeed<'de> for Tag<'_> {
        type Value = Vec<String>;
        fn deserialize<D: Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> Result<Self::Value, D::Error> {
            deserializer.deserialize_seq(self)
        }
    }
    impl<'de> Visitor<'de> for Tag<'_> {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("bounded tag strings")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let hint = seq.size_hint();
            let mut values = Vec::new();
            if let Some(count) = hint {
                self.0.nodes(count)?;
                self.0.reserve(&mut values, count)?;
            }
            while let Some(value) = seq.next_element_seed(Text(self.0))? {
                if let Some(count) = hint {
                    if values.len() >= count {
                        return Err(de::Error::custom("tag length exceeded its declared bound"));
                    }
                } else {
                    self.0.nodes(1)?;
                    self.0.reserve(&mut values, 1)?;
                }
                values.push(value);
            }
            if hint.is_some_and(|count| values.len() != count) {
                return Err(de::Error::custom("tag length is incomplete"));
            }
            Ok(values)
        }
    }

    struct Tags;
    impl<'de> Visitor<'de> for Tags {
        type Value = Vec<Vec<String>>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("bounded complete event tags")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut budget = Budget::default();
            let mut values = Vec::new();
            let hint = seq.size_hint();
            if let Some(count) = hint {
                budget.nodes(count)?;
                budget.reserve(&mut values, count)?;
            }
            while let Some(value) = seq.next_element_seed(Tag(&mut budget))? {
                if let Some(count) = hint {
                    if values.len() >= count {
                        return Err(de::Error::custom("tags length exceeded its declared bound"));
                    }
                } else {
                    budget.nodes(1)?;
                    budget.reserve(&mut values, 1)?;
                }
                values.push(value);
            }
            if hint.is_some_and(|count| values.len() != count) {
                return Err(de::Error::custom("tags length is incomplete"));
            }
            Ok(values)
        }
    }

    pub fn deserialize<'de, 'a, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Cow<'a, [Vec<String>]>, D::Error> {
        deserializer.deserialize_seq(Tags).map(Cow::Owned)
    }
}
