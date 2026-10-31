//! Persisted policy format identity, shared by the closed codec and SQL readers.
//! Header recognition is not validation of settings or acquisition authority.
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyDocumentVersion {
    V3,
}

impl PolicyDocumentVersion {
    pub const CURRENT: Self = Self::V3;

    pub const fn format(self) -> &'static str {
        match self {
            Self::V3 => "bigbrotr-product-policy/3",
        }
    }

    pub const fn digest_domain(self) -> &'static [u8] {
        match self {
            Self::V3 => b"bigbrotr/product-policy/3\0",
        }
    }

    pub const fn schema_version(self) -> i32 {
        match self {
            Self::V3 => 3,
        }
    }

    pub fn from_format(format: &str) -> Result<Self, &'static str> {
        match format {
            "bigbrotr-product-policy/3" => Ok(Self::V3),
            _ => Err("incompatible stored policy format"),
        }
    }

    pub fn from_metadata(schema: i32, format: &str) -> Result<Self, &'static str> {
        let version = Self::from_format(format)?;
        if version.schema_version() != schema {
            return Err("policy format and schema metadata disagree");
        }
        Ok(version)
    }

    pub fn from_document(document: &str) -> Result<Self, &'static str> {
        if document.len() > 10 * 1024 * 1024 {
            return Err("stored policy exceeds encoded limit");
        }
        // Scan settings without allocating another Value tree. The owning codec
        // still checks its complete, version-specific normalized field set.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Header<'a> {
            #[serde(borrow)]
            format: std::borrow::Cow<'a, str>,
            #[serde(rename = "settings")]
            _settings: serde::de::IgnoredAny,
        }
        let header: Header<'_> = serde_json::from_str(document)
            .map_err(|_| "invalid stored policy encoding (values withheld)")?;
        Self::from_format(&header.format)
    }

    pub fn require_metadata(document: &str, schema: i32) -> Result<Self, &'static str> {
        let version = Self::from_document(document)?;
        Self::from_metadata(schema, version.format())
    }
}

#[cfg(test)]
mod tests {
    use super::PolicyDocumentVersion as Version;

    #[test]
    fn policy_header_requires_known_format_and_matching_metadata() {
        let version = Version::V3;
        let document = format!(r#"{{"format":"{}","settings":{{}}}}"#, version.format());
        assert_eq!(Version::require_metadata(&document, 3), Ok(version));
        for wrong in [0, 1, 2, 65536] {
            assert!(Version::require_metadata(&document, wrong).is_err());
        }
        for invalid in [
            r#"{"format":"bigbrotr-product-policy/1","settings":{}}"#,
            r#"{"format":"bigbrotr-product-policy/2","settings":{}}"#,
            r#"{"format":"bigbrotr-product-policy/3"}"#,
            r#"{"format":"bigbrotr-product-policy/3","format":"bigbrotr-product-policy/3","settings":{}}"#,
            r#"{"format":"bigbrotr-product-policy/3","settings":{},"extra":1}"#,
            r#"{"format":"bigbrotr-product-policy/3","settings":{},"settings":{}}"#,
        ] {
            assert!(Version::from_document(invalid).is_err());
        }
    }
}
