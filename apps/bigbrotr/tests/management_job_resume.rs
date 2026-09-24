//! Command identity only; actual resume/CAS/retry requires PostgreSQL evidence.
use bigbrotr::management::jobs::ResumeExportCommand;
use serde_json::Value;

fn vectors() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/management/export-resume.json"
    ))
    .unwrap()
}

#[test]
fn resume_fingerprint_binds_original_actor_job_key_and_all_revisions() {
    let fixture = vectors();
    for vector in fixture["vectors"].as_array().unwrap() {
        let digest = |v: &Value| {
            serde_json::from_value::<ResumeExportCommand>(v["command"].clone())
                .unwrap()
                .digest(
                    v["dataset"].as_str().unwrap().parse().unwrap(),
                    v["actor"].as_str().unwrap().parse().unwrap(),
                    v["command_key"].as_str().unwrap().parse().unwrap(),
                    v["job_id"].as_str().unwrap().parse().unwrap(),
                )
        };
        let expected = digest(vector);
        assert_eq!(expected.to_string(), vector["sha256"]);
        for identity in ["dataset", "actor", "command_key", "job_id"] {
            let mut other = vector.clone();
            other[identity] = "ff".repeat(16).into();
            assert_ne!(digest(&other), expected, "{identity}");
        }
        for revision in [
            "expected_job_revision",
            "expected_config_revision",
            "expected_policy_revision",
        ] {
            let mut other = vector.clone();
            other["command"][revision] = "1".into();
            assert_ne!(digest(&other), expected, "{revision}");
        }
    }
}

#[test]
fn resume_rejects_missing_ambiguous_and_mutable_job_inputs() {
    let command = vectors()["vectors"][0]["command"].clone();
    for field in [
        "expected_job_revision",
        "expected_config_revision",
        "expected_policy_revision",
    ] {
        let mut absent = command.clone();
        absent.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<ResumeExportCommand>(absent).is_err());
        for value in ["0", "04", "+4", "-4", "18446744073709551616"] {
            let mut invalid = command.clone();
            invalid[field] = value.into();
            assert!(serde_json::from_value::<ResumeExportCommand>(invalid).is_err());
        }
        let wire = command.to_string();
        let duplicate = wire.replacen('{', &format!("{{\"{field}\":\"4\","), 1);
        assert!(serde_json::from_str::<ResumeExportCommand>(&duplicate).is_err());
    }
    for field in [
        "query",
        "destination",
        "max_output_bytes",
        "job_id",
        "principal_id",
    ] {
        let prefix = format!("{{\"{field}\":");
        let body = format!("{prefix}[{}]}}", vec!["[0]"; 1500].join(","));
        let mut reader = std::io::Cursor::new(body.as_bytes());
        assert!(serde_json::from_reader::<_, ResumeExportCommand>(&mut reader).is_err());
        assert!(reader.position() <= prefix.len() as u64, "{field}");
    }
}
