use super::worker::PreparationError;
use serde::Serialize;
use std::io::{self, Write};

/// Borrowed secrets are serialized directly onto the private supervised pipe.
#[derive(Serialize)]
pub(super) struct Input<'a> {
    pub(super) dsn: &'a str,
    pub(super) data_root: &'a str,
}
impl Input<'_> {
    pub(super) fn validate(&self) -> Result<(), PreparationError> {
        if self.dsn.is_empty()
            || self.dsn.len() > 16_384
            || self.dsn.contains(['\0', '\n', '\r'])
            || self.data_root.len() < 2
            || self.data_root.len() > 4096
            || !self.data_root.starts_with('/')
            || !self.data_root.ends_with('/')
            || self.data_root.contains(['\0', '\n', '\r'])
            // Native file registration treats even a singleton path as a glob.
            || self.data_root.contains(['*', '?', '['])
            || self.data_root.contains("//")
            || self
                .data_root
                .split('/')
                .any(|part| matches!(part, "." | ".."))
        {
            return Err(PreparationError::InvalidBootstrap);
        }
        // Count the actual escaping before starting a child. No plaintext JSON
        // buffer is allocated; the second serialization streams onto its pipe.
        serde_json::to_writer(WireLimit(0), self)
            .map_err(|_| PreparationError::InvalidBootstrap)?;
        Ok(())
    }
}

struct WireLimit(usize);

pub(super) fn decode_response(output: &[u8], format: &str) -> Result<(), PreparationError> {
    // Decode the closed object directly. A map would collapse duplicate keys
    // before validation and could hide an earlier conflicting completion.
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Response<'a> {
        format: &'a str,
        status: &'a str,
    }
    let value: Response<'_> =
        serde_json::from_slice(output).map_err(|_| PreparationError::InvalidResult)?;
    if value.format != format || value.status != "initialized" {
        return Err(PreparationError::InvalidResult);
    }
    Ok(())
}
impl Write for WireLimit {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|n| *n <= 32_768)
            .ok_or_else(|| io::Error::other("bootstrap input limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_completion_refuses_duplicate_fields_even_when_the_last_value_matches() {
        for format in [
            "bigbrotr-native-bootstrap/1",
            "bigbrotr-native-initialize/1",
        ] {
            let valid = format!(r#"{{"format":"{format}","status":"initialized"}}"#);
            assert_eq!(decode_response(valid.as_bytes(), format), Ok(()));
            for extra in [
                r#""status":"failed""#.to_owned(),
                r#""status":"initialized""#.to_owned(),
                r#""format":"unknown""#.to_owned(),
                format!(r#""format":"{format}""#),
                r#""unknown":null"#.to_owned(),
            ] {
                let invalid = format!("{{{extra},{}", &valid[1..]);
                assert_eq!(
                    decode_response(invalid.as_bytes(), format),
                    Err(PreparationError::InvalidResult),
                    "{invalid}"
                );
            }
            for invalid in [
                format!("{valid}{{}}"),
                format!(r#"{{"format":"{format}"}}"#),
                format!(r#"{{"format":"{format}","status":null}}"#),
                format!(r#"{{"format":"{format}","status":"failed"}}"#),
            ] {
                assert_eq!(
                    decode_response(invalid.as_bytes(), format),
                    Err(PreparationError::InvalidResult)
                );
            }
        }
    }

    #[test]
    fn bootstrap_rejects_unbounded_or_noncanonical_input_before_spawn() {
        for dsn in [
            "".into(),
            "x\n".into(),
            "x\0".into(),
            "x".repeat(16_385),
            "\t".repeat(16_384),
        ] {
            assert!(
                Input {
                    dsn: &dsn,
                    data_root: "/data/"
                }
                .validate()
                .is_err()
            );
        }
        for root in [
            "",
            "/",
            "relative/",
            "/data",
            "/data//",
            "/data/./",
            "/data/../",
            "/data\n/",
            "/data/*/",
            "/data/?/",
            "/data/[x]/",
        ] {
            assert!(
                Input {
                    dsn: "host=localhost",
                    data_root: root
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            Input {
                dsn: "host=localhost",
                data_root: &format!("/{}/", "a".repeat(4095))
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn private_wire_preserves_quoted_credentials_and_unicode_root() {
        let input = Input {
            dsn: "host=localhost password='a\\b\"c'",
            data_root: "/data/è/",
        };
        input.validate().unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&input).unwrap()).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"dsn":"host=localhost password='a\\b\"c'", "data_root":"/data/è/"})
        );
        assert!(
            Input {
                dsn: &"x".repeat(16_384),
                data_root: "/data/"
            }
            .validate()
            .is_ok()
        );
    }
}
