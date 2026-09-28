use super::{Encoder, Error, Write, hash};
use crate::{
    Digest256, NostrTime,
    acquisition::{CollectionSelector, MAX_PREIMAGE_BYTES, SelectorError},
};

fn optional<W: Write, T>(
    encoder: &mut Encoder<W>,
    values: Option<&[T]>,
    mut item: impl FnMut(&mut Encoder<W>, &T) -> Result<(), Error<W::Error>>,
) -> Result<(), Error<W::Error>> {
    if let Some(values) = values {
        encoder.array(values.len() as u64)?;
        for value in values {
            item(encoder, value)?;
        }
    } else {
        encoder.null()?;
    }
    Ok(())
}

fn time<W: Write>(
    encoder: &mut Encoder<W>,
    value: Option<NostrTime>,
) -> Result<(), Error<W::Error>> {
    match value {
        Some(value) => encoder.u64(value.0)?.ok(),
        None => encoder.null()?.ok(),
    }
}

fn encode<W: Write>(
    value: &CollectionSelector,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(b"bigbrotr/collection-selector/1\0")
        .map_err(Error::write)?;
    encoder.array(3)?.array(value.filters().len() as u64)?;
    for filter in value.filters() {
        let f = filter.fields();
        encoder.array(6)?;
        optional(encoder, f.ids.as_deref(), |e, id| {
            e.bytes(id.as_bytes())?.ok()
        })?;
        optional(encoder, f.authors.as_deref(), |e, key| {
            e.bytes(key.as_bytes())?.ok()
        })?;
        optional(encoder, f.kinds.as_deref(), |e, kind| e.u16(*kind)?.ok())?;
        encoder.array(f.tags.len() as u64)?;
        for (key, values) in &f.tags {
            let bytes = [*key as u8];
            let key = std::str::from_utf8(&bytes).expect("validated ASCII tag key");
            encoder.array(2)?.str(key)?.array(values.len() as u64)?;
            for value in values {
                encoder.str(value)?;
            }
        }
        time(encoder, f.since)?;
        time(encoder, f.until)?;
    }
    encoder
        .u64(value.history_since().0)?
        .str(value.ephemeral().as_str())?
        .ok()
}

struct Count(usize);
impl Write for Count {
    type Error = SelectorError;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|count| *count <= MAX_PREIMAGE_BYTES)
            .ok_or(SelectorError::CanonicalLimit)?;
        Ok(())
    }
}

pub(crate) fn check_size(value: &CollectionSelector) -> Result<(), SelectorError> {
    encode(value, &mut Encoder::new(Count(0))).map_err(|_| SelectorError::CanonicalLimit)
}

impl CollectionSelector {
    /// Stable collection intent only; no registration, permission or coverage.
    pub fn digest(&self) -> Digest256 {
        hash(|encoder| encode(self, encoder))
    }
}
