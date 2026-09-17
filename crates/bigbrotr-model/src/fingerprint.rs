//! Closed profile-1 recipes, not a general CBOR serializer. Only named typed
//! fields can reach the codec; no generic values, floats, maps or extension bags.
use crate::{AdmissionBatch, Digest256, SignedEvent, SourceRecordId};
use minicbor::{
    Encoder,
    encode::{Error, Write},
};
use sha2::{Digest, Sha256};
use std::convert::Infallible;

struct HashWriter(Sha256);
impl Write for HashWriter {
    type Error = Infallible;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0.update(bytes);
        Ok(())
    }
}

fn hash(
    recipe: impl FnOnce(&mut Encoder<HashWriter>) -> Result<(), Error<Infallible>>,
) -> Digest256 {
    let mut encoder = Encoder::new(HashWriter(Sha256::new()));
    // These closed recipes call only primitive encoder methods. Their only
    // error path is Write::Error; the SHA-256 writer is infallible.
    recipe(&mut encoder).expect("closed fingerprint recipe cannot fail its digest writer");
    Digest256::from_bytes(encoder.into_writer().0.finalize().into())
}

fn body<W: Write>(event: &SignedEvent, encoder: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(b"bigbrotr/body/1\0")
        .map_err(Error::write)?;
    encoder
        .array(6)?
        .bytes(event.id.as_bytes())?
        .bytes(event.pubkey.as_bytes())?
        .u64(event.created_at.0)?
        .u16(event.kind)?
        .array(event.tags.len() as u64)?;
    for tag in &event.tags {
        encoder.array(tag.len() as u64)?;
        for value in tag {
            encoder.str(value)?;
        }
    }
    encoder.str(&event.content)?.ok()
}

fn source<W: Write>(id: SourceRecordId, encoder: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
    encoder
        .array(4)?
        .bytes(id.dataset_id.as_bytes())?
        .u32(id.shard_id)?
        .u64(id.owner_epoch.get())?
        .u64(id.sequence.get())?
        .ok()
}

fn admission<W: Write>(
    batch: &AdmissionBatch,
    encoder: &mut Encoder<W>,
) -> Result<(), Error<W::Error>> {
    encoder
        .writer_mut()
        .write_all(b"bigbrotr/admission-batch/1\0")
        .map_err(Error::write)?;
    let first = batch.first();
    encoder
        .array(6)?
        .bytes(first.dataset_id.as_bytes())?
        .u32(first.shard_id)?
        .u64(first.owner_epoch.get())?
        .u64(first.sequence.get())?
        .u64(batch.last().sequence.get())?
        .array(batch.records().len() as u64)?;
    for record in batch.records() {
        encoder.array(2)?;
        source(record.source_record_id, encoder)?;
        encoder.bytes(record.payload_digest.as_bytes())?;
    }
    Ok(())
}

impl SignedEvent {
    /// Structural shared-payload fingerprint, excluding signature and envelope.
    /// This does not verify the event; admission must use the protocol verifier.
    pub fn body_digest(&self) -> Digest256 {
        hash(|encoder| body(self, encoder))
    }
}
impl AdmissionBatch {
    /// Semantic membership fingerprint, not proof of an engine commit.
    pub fn digest(&self) -> Digest256 {
        hash(|encoder| admission(self, encoder))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatasetId, FactRef, NostrTime, OwnerEpoch, Sequence, Signature};

    struct Bytes(Vec<u8>);
    impl Write for Bytes {
        type Error = Infallible;
        fn write_all(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
            self.0.extend_from_slice(bytes);
            Ok(())
        }
    }
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn multi_record_membership_and_endpoints_cross_integer_width_boundary() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/model/admission-two-records.json"
        ))
        .unwrap();
        let dataset = fixture["dataset_id"].as_str().unwrap().parse().unwrap();
        let records = fixture["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| FactRef {
                source_record_id: SourceRecordId {
                    dataset_id: dataset,
                    shard_id: 0,
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                    sequence: Sequence::new(row["sequence"].as_str().unwrap().parse().unwrap())
                        .unwrap(),
                },
                payload_digest: row["payload_digest"].as_str().unwrap().parse().unwrap(),
            })
            .collect();
        let batch = AdmissionBatch::new(records).unwrap();
        let mut out = Encoder::new(Bytes(Vec::new()));
        admission(&batch, &mut out).unwrap();
        assert_eq!(hex(&out.into_writer().0), fixture["preimage_hex"]);
        assert_eq!(batch.digest().to_string(), fixture["sha256"]);
        let truncated = AdmissionBatch::new(vec![batch.records()[0]]).unwrap();
        assert_ne!(truncated.digest(), batch.digest());
    }

    #[test]
    fn exact_body_and_admission_bytes_match_independently_assembled_vectors() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../experiments/identity-contract/golden-vectors.json"
        ))
        .unwrap();
        let row = |name: &str| {
            fixture["vectors"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["name"] == name)
                .unwrap()
        };
        let value = &row("body-u64-ordered-tags")["value"];
        let event = SignedEvent {
            id: value[0]["$bytes"].as_str().unwrap().parse().unwrap(),
            pubkey: value[1]["$bytes"].as_str().unwrap().parse().unwrap(),
            created_at: NostrTime(value[2]["$int"].as_str().unwrap().parse().unwrap()),
            kind: value[3].as_u64().unwrap().try_into().unwrap(),
            tags: serde_json::from_value(value[4].clone()).unwrap(),
            content: value[5].as_str().unwrap().into(),
            sig: Signature::from_bytes([0; 64]),
        };
        let mut out = Encoder::new(Bytes(Vec::new()));
        body(&event, &mut out).unwrap();
        assert_eq!(
            hex(&out.into_writer().0),
            row("body-u64-ordered-tags")["preimage_hex"]
        );
        let batch = AdmissionBatch::new(vec![FactRef {
            source_record_id: SourceRecordId {
                dataset_id: DatasetId::try_from([1; 16]).unwrap(),
                shard_id: 0,
                owner_epoch: OwnerEpoch::new(1).unwrap(),
                sequence: Sequence::new(1).unwrap(),
            },
            payload_digest: row("fact-eligibility")["sha256"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
        }])
        .unwrap();
        let mut out = Encoder::new(Bytes(Vec::new()));
        admission(&batch, &mut out).unwrap();
        assert_eq!(
            hex(&out.into_writer().0),
            row("admission-batch")["preimage_hex"]
        );
    }
}
