//! Bounded projection of already validated, accepted startup policy.
//! This value is not authority: registration binds every captured setting to
//! both exact accepted revision/digest identities before it permits work.
use serde_json::Value;
use std::{collections::BTreeMap, fmt, time::Duration};

const PENDING: &str = "ingest.pending_max_bytes";
const HIGH: &str = "ingest.pending_high_fraction";
const LOW: &str = "ingest.pending_low_fraction";
const UNIT: &str = "ingest.group_commit_max_bytes";
const MARGIN: &str = "runtime.disk_resume_margin_bytes";
const STABLE: &str = "runtime.pressure_stable_duration";
const RETENTION: &str = "ingest.request_receipt_retention";
const KEYS: [&str; 7] = [PENDING, HIGH, LOW, UNIT, MARGIN, STABLE, RETENTION];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntakePolicyError;
impl fmt::Display for IntakePolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("accepted intake policy is missing, invalid or cannot fund one supported unit")
    }
}
impl std::error::Error for IntakePolicyError {}

/// One immutable startup capture, retained with InitialRegistration's exact
/// configuration/security revision and digests. There is no default or mutable
/// per-event policy view. Capturing it does not install a logical/disk grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedIntakePolicy {
    pending_limit: u64,
    pending_high: u64,
    pending_low: u64,
    max_pending_unit: u64,
    disk_resume_margin: u64,
    stable_duration: Duration,
    receipt_retention: u8,
    // The original normalized numbers, including fractions, are checked against
    // both accepted documents by the existing registration identity query.
    binding: String,
}
impl CapturedIntakePolicy {
    /// Convert the captured projection without substituting another accepted
    /// pair. This is still a policy value, not native funding or SQL authority.
    pub fn pending_quota_policy(
        &self,
        accepted: crate::storage::postgres::control_owner::PolicySnapshot,
    ) -> Result<crate::storage::rocks::store::PendingQuotaPolicy, IntakePolicyError> {
        crate::storage::rocks::store::PendingQuotaPolicy::new(
            self.pending_high,
            self.pending_low,
            self.max_pending_unit,
            accepted.config_revision,
            bigbrotr_model::Digest256::from_bytes(accepted.config_digest),
            accepted.policy_revision,
            bigbrotr_model::Digest256::from_bytes(accepted.policy_digest),
            self.stable_duration,
        )
        .map_err(|_| IntakePolicyError)
    }

    /// Called after the existing complete-document snapshot validation in the
    /// same transaction. Activation uses its current accepted pair; a previous
    /// registrar's projection cannot supply a new writer's quota policy.
    pub(in crate::storage::postgres) async fn load_accepted(
        tx: &tokio_postgres::Transaction<'_>,
        dataset: bigbrotr_model::DatasetId,
        accepted: crate::storage::postgres::control_owner::PolicySnapshot,
    ) -> Result<Self, crate::storage::postgres::control_owner::OwnerError> {
        use crate::storage::postgres::control_owner::OwnerError;
        let row = tx
            .query_opt(
                "SELECT left((SELECT jsonb_object_agg(key,value)
                     FROM jsonb_each(p.document->'settings') WHERE key=ANY($6::text[]))::text,2049),
                    NOT EXISTS(SELECT 1 FROM unnest($6::text[]) AS k(key)
                        WHERE c.document->'settings'->k.key IS DISTINCT FROM p.document->'settings'->k.key
                           OR p.document->'settings'->k.key IS NULL)
                 FROM control.config_revisions c JOIN control.policy_revisions p
                   ON p.dataset_id=c.dataset_id AND p.config_revision=c.revision
                 WHERE c.dataset_id=$1::bytea AND c.revision=$2::text::numeric
                   AND p.revision=$3::text::numeric AND c.document_digest=$4::bytea
                   AND p.document_digest=$5::bytea",
                &[
                    &&dataset.as_bytes()[..],
                    &accepted.config_revision.to_string(),
                    &accepted.policy_revision.to_string(),
                    &&accepted.config_digest[..],
                    &&accepted.policy_digest[..],
                    &&KEYS[..],
                ],
            )
            .await
            .map_err(|_| OwnerError::Backend)?
            .ok_or(OwnerError::InvalidPolicy)?;
        if !row
            .try_get::<_, bool>(1)
            .map_err(|_| OwnerError::InvalidPolicy)?
        {
            return Err(OwnerError::InvalidPolicy);
        }
        let projection: &str = row.try_get(0).map_err(|_| OwnerError::InvalidPolicy)?;
        if projection.len() > 2048 {
            return Err(OwnerError::InvalidPolicy);
        }
        let settings = serde_json::from_str(projection).map_err(|_| OwnerError::InvalidPolicy)?;
        Self::capture(&settings).map_err(|_| OwnerError::InvalidPolicy)
    }

    /// Extract only the closed intake projection from normalized settings that
    /// the caller has restored and validated. No omitted key is synthesized.
    /// The constructor alone grants no ownership or permission to stage/write.
    pub fn capture(settings: &BTreeMap<String, Value>) -> Result<Self, IntakePolicyError> {
        let number = |key| {
            settings
                .get(key)
                .and_then(Value::as_u64)
                .filter(|&value| value > 0)
                .ok_or(IntakePolicyError)
        };
        let pending_limit = number(PENDING)?;
        let pending_high = fraction_floor(pending_limit, settings.get(HIGH))?;
        let pending_low = fraction_floor(pending_limit, settings.get(LOW))?;
        let max_pending_unit = number(UNIT)?;
        let disk_resume_margin = number(MARGIN)?;
        let stable_duration = Duration::from_micros(number(STABLE)?);
        let receipt_retention = u8::try_from(number(RETENTION)?)
            .ok()
            .filter(|value| (1..=64).contains(value))
            .ok_or(IntakePolicyError)?;
        if pending_low == 0
            || pending_low >= pending_high
            || pending_high > pending_limit
            || max_pending_unit >= pending_high
        {
            return Err(IntakePolicyError);
        }
        let projection: BTreeMap<_, _> =
            KEYS.into_iter().map(|key| (key, &settings[key])).collect();
        let binding = serde_json::to_string(&projection).map_err(|_| IntakePolicyError)?;
        Ok(Self {
            pending_limit,
            pending_high,
            pending_low,
            max_pending_unit,
            disk_resume_margin,
            stable_duration,
            receipt_retention,
            binding,
        })
    }
    pub const fn pending_limit(&self) -> u64 {
        self.pending_limit
    }
    pub const fn pending_high(&self) -> u64 {
        self.pending_high
    }
    pub const fn pending_low(&self) -> u64 {
        self.pending_low
    }
    /// Sufficient upper bound from the accepted serialized group-commit limit;
    /// it is not the exact scientific P delta of a prepared operation.
    pub const fn max_pending_unit(&self) -> u64 {
        self.max_pending_unit
    }
    pub const fn disk_resume_margin(&self) -> u64 {
        self.disk_resume_margin
    }
    pub const fn stable_duration(&self) -> Duration {
        self.stable_duration
    }
    pub const fn request_receipt_retention(&self) -> u8 {
        self.receipt_retention
    }
    pub(super) fn settings_binding(&self) -> &str {
        &self.binding
    }
}

/// Apply the accepted JSON number's decimal value exactly, avoiding a lossy
/// u64->f64 multiplication at large limits. The repository's normalized number
/// representation is a finite round-trip f64 (serde_json float_roundtrip).
fn fraction_floor(limit: u64, value: Option<&Value>) -> Result<u64, IntakePolicyError> {
    let number = value.and_then(Value::as_number).ok_or(IntakePolicyError)?;
    if number
        .as_f64()
        .is_none_or(|value| !(0.0 < value && value < 1.0))
    {
        return Err(IntakePolicyError);
    }
    let encoded = number.to_string();
    let (mantissa, exponent) = match encoded.split_once('e') {
        None => (encoded.as_str(), 0),
        Some((mantissa, exponent)) => (
            mantissa,
            exponent.parse::<i32>().map_err(|_| IntakePolicyError)?,
        ),
    };
    let mut coefficient = 0u128;
    let mut fractional_digits = 0i32;
    let mut after_point = false;
    for byte in mantissa.bytes() {
        match byte {
            b'.' if !after_point => after_point = true,
            b'0'..=b'9' => {
                coefficient = coefficient
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(u128::from(byte - b'0')))
                    .ok_or(IntakePolicyError)?;
                if after_point {
                    fractional_digits += 1;
                }
            }
            _ => return Err(IntakePolicyError),
        }
    }
    let scale = fractional_digits
        .checked_sub(exponent)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(IntakePolicyError)?;
    let product = u128::from(limit)
        .checked_mul(coefficient)
        .ok_or(IntakePolicyError)?;
    // An unrepresentably large positive denominator exceeds the checked
    // numerator. Its exact floor is zero, which capture explicitly refuses.
    let value = 10u128
        .checked_pow(scale)
        .map_or(0, |denominator| product / denominator);
    u64::try_from(value).map_err(|_| IntakePolicyError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn settings() -> BTreeMap<String, Value> {
        serde_json::from_value(json!({
            "ingest.pending_max_bytes": 274_877_906_944u64,
            "ingest.pending_high_fraction": 0.9, "ingest.pending_low_fraction": 0.75,
            "ingest.group_commit_max_bytes": 4_194_304,
            "runtime.disk_resume_margin_bytes": 17_179_869_184u64,
            "runtime.pressure_stable_duration": 30_000_000,
            "ingest.request_receipt_retention": 64
        }))
        .unwrap()
    }
    #[test]
    fn current_capture_preserves_original_policy_and_exact_decimal_floors() {
        let values = settings();
        let captured = CapturedIntakePolicy::capture(&values).unwrap();
        assert_eq!(captured.pending_limit(), 274_877_906_944);
        assert_eq!(captured.pending_high(), 247_390_116_249);
        assert_eq!(captured.pending_low(), 206_158_430_208);
        assert_eq!(captured.max_pending_unit(), 4_194_304);
        assert_eq!(captured.disk_resume_margin(), 17_179_869_184);
        assert_eq!(captured.stable_duration(), Duration::from_secs(30));
        assert_eq!(captured.request_receipt_retention(), 64);
        let restored = serde_json::from_str(captured.settings_binding()).unwrap();
        assert_eq!(CapturedIntakePolicy::capture(&restored).unwrap(), captured);
        assert_eq!(
            fraction_floor(u64::MAX, Some(&json!(0.9))),
            Ok(16_602_069_666_338_596_453)
        );
        assert_eq!(fraction_floor(u64::MAX, Some(&json!(1e-20))), Ok(0));
        assert_eq!(fraction_floor(u64::MAX, Some(&json!(1e-300))), Ok(0));
    }
    #[test]
    fn missing_invalid_or_unfundable_projection_never_gets_defaults() {
        for key in KEYS {
            let mut values = settings();
            values.remove(key);
            assert_eq!(
                CapturedIntakePolicy::capture(&values),
                Err(IntakePolicyError)
            );
        }
        for (key, value) in [
            (RETENTION, json!(0)),
            (RETENTION, json!(65)),
            (LOW, json!(0)),
            (LOW, json!(0.95)),
            (HIGH, json!(1)),
            (LOW, json!(1e-20)),
            (HIGH, json!("0.9")),
            (STABLE, json!(0)),
            (MARGIN, json!(0)),
            (PENDING, json!(0)),
            (UNIT, json!(247_390_116_249u64)),
        ] {
            let mut values = settings();
            values.insert(key.into(), value);
            assert_eq!(
                CapturedIntakePolicy::capture(&values),
                Err(IntakePolicyError)
            );
        }
    }
    #[test]
    fn all_retention_thresholds_are_explicit_and_equal_rounded_limits_do_not_alias_policy() {
        let mut values = settings();
        for count in 1..=64 {
            values.insert(RETENTION.into(), json!(count));
            assert_eq!(
                CapturedIntakePolicy::capture(&values)
                    .unwrap()
                    .request_receipt_retention(),
                count
            );
        }
        let original = CapturedIntakePolicy::capture(&values).unwrap();
        values.insert(HIGH.into(), json!(0.9000000000001));
        let changed = CapturedIntakePolicy::capture(&values).unwrap();
        assert_eq!(original.pending_high(), changed.pending_high());
        assert_ne!(original, changed);
    }
}
