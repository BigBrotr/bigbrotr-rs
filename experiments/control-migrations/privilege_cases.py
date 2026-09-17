#!/usr/bin/env python3
"""Exercise actual migration-role separation in the owned PostgreSQL lab."""
import argparse
import hashlib
import json
from pathlib import Path
import re

import lab


def run(root, name):
    state = lab.load(root)
    database = "bb_" + name + "_privileges"
    output = root / (name + "-privileges.json")
    if output.exists():
        raise RuntimeError("refusing to overwrite privilege evidence")
    report = {"status": "RUNNING", "cases": [], "binary_sha256": state["binary_sha256"],
              "application_source": state["build"]["build_info"]["source"],
              "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}

    def record(case, passed, observed):
        report["cases"].append({"name": case, "status": "PASS" if passed else "FAIL", "observed": observed})
        lab.write_json(output, report)
        if not passed:
            raise RuntimeError("privilege acceptance failed: " + case)

    def application(statement):
        return lab.docker("exec", "-i", state["postgres"], "psql", "-X", "-A", "-t", "-q", "--no-password",
                          "-U", "bigbrotr_control", "-d", database, "-v", "ON_ERROR_STOP=1",
                          "-v", "VERBOSITY=sqlstate", data=statement, check=False)

    try:
        lab.sql(state, "postgres", f"CREATE DATABASE {database} OWNER bigbrotr_migrator;")
        candidate = lab.config(root, state, database)
        # Bypass only the static name comparison in this laboratory to prove the
        # engine refuses real runtime credentials independently of that check.
        path = root / Path(candidate).name
        path.write_text(lab.change(path.read_text(), "postgres.control", "user", "different_configured_application"))
        inspect = lab.migrate(state, "inspect", candidate, user="bigbrotr_control", password="application")
        result = lab.migrate(state, "apply", candidate, user="bigbrotr_control", password="application")
        count = lab.sql(state, database, "SELECT count(*) FROM pg_namespace WHERE nspname='control';")
        record("runtime_login_cannot_initialize_schema", inspect.returncode == 0 and result.returncode == 1 and count == "0",
               {"positive_inspection_exit": inspect.returncode, "exit": result.returncode, "namespace_count": count, "stderr": result.stderr})
        result = lab.migrate(state, "apply", candidate)
        record("dedicated_login_can_initialize_same_database", result.returncode == 0 and json.loads(result.stdout).get("status") == "applied",
               {"exit": result.returncode})
        # No runtime mutation adapter exists yet. Grant only the reads used by
        # this fixture, not a fictitious future complete role policy.
        lab.sql(state, database, "GRANT USAGE ON SCHEMA control TO bigbrotr_control; GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control;")
        result = application("SELECT count(*) FROM control.schema_versions;")
        record("runtime_login_has_intended_read_access", result.returncode == 0 and result.stdout.strip() == "1", {"exit": result.returncode, "rows": result.stdout.strip()})
        baseline = lab.schema_digest(state, database)
        for case, statement in [
            ("no_schema_create", "CREATE SCHEMA runtime_created;"),
            ("no_table_create", "CREATE TABLE control.runtime_created(id integer);"),
            ("no_ledger_write", "UPDATE control.schema_versions SET applied_at_us=0;"),
            ("no_truncate", "TRUNCATE control.schema_versions;"),
            ("no_trigger_disable", "ALTER TABLE control.schema_versions DISABLE TRIGGER ALL;"),
            ("no_ownership_change", "ALTER TABLE control.schema_versions OWNER TO bigbrotr_control;"),
            ("no_role_elevation", "ALTER ROLE bigbrotr_control SUPERUSER;"),
        ]:
            result = application(statement)
            unchanged = lab.schema_digest(state, database) == baseline
            record(case, result.returncode != 0 and re.search(r"ERROR:\s+42501\b", result.stderr) is not None and unchanged,
                   {"exit": result.returncode, "sqlstate": "42501" if "42501" in result.stderr else "unexpected", "schema_and_data_unchanged": unchanged})
        roles = json.loads(lab.sql(state, database, "SELECT json_agg(json_build_object('role',rolname,'superuser',rolsuper,'create_role',rolcreaterole,'create_database',rolcreatedb)) FROM pg_roles WHERE rolname IN ('bigbrotr_control','bigbrotr_migrator');"))
        record("neither_login_has_administrative_role_flags", len(roles) == 2 and all(not r[k] for r in roles for k in ['superuser', 'create_role', 'create_database']), roles)
        report["status"] = "PASS"
    except Exception:
        report["status"] = "FAIL"
        raise
    finally:
        lab.write_json(output, report)
    print(json.dumps({"status": report["status"], "cases": len(report["cases"]), "output": str(output)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-name", default="roles")
    args = parser.parse_args()
    if not args.root.is_absolute() or not re.fullmatch(r"[a-z_]{1,20}", args.run_name):
        parser.error("absolute owned root and bounded lowercase run name required")
    run(args.root, args.run_name)
