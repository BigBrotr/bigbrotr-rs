"""Finite SQL-only qualification of the first Index layout on an owned server.

Python stdlib + Docker; no product/native build and no foreign database adoption.
Raw outputs and generated credentials stay below a fresh mode0700 --lab-root.
The source fingerprints deliberately limit this recipe to the reviewed schema.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import time
import traceback
import uuid

IMAGE = "clickhouse/clickhouse-server:26.8.35.10@sha256:324e2f39fa3be72b5838acf05af0f3cd0bfcd90f0096a9b0cf470b757fc355cc"
VERSION = "26.8.35.10"
SCHEMA_SHA = "b83807768847a743f0e585831c81cba699e9ba789595d3899e155c062b96469c"
INSPECT_SHA = "74cd53ef21e11e23a599b0bf4e8e87bdc60fc5596cf655661fa6e3e5d3e165f5"
DB = "serving_fixture"
SETTINGS = "fsync_after_insert = 1, fsync_part_directory = 1, non_replicated_deduplication_window = 0, index_granularity = 8192"
SOURCE = "Tuple(FixedString(16), UInt32, UInt64, UInt64)"
REF = "Tuple(FixedString(16), UInt16, FixedString(16), UInt64)"
OPT_REF = f"Array({REF})"
COMMON = [
    ("projection_generation", "FixedString(16)"), ("source", SOURCE),
    ("record_schema_version", "UInt16"), ("visibility_scope", REF),
    ("recorded_at", "Int64"), ("payload_digest", "FixedString(32)"),
    ("discriminator", "UInt64"),
]
DICTIONARY = [
    ("dictionary_ref", REF), ("normalization_version", "UInt16"),
    ("descriptor_schema", "String"), ("descriptor_version", "UInt16"),
    ("descriptor_digest", "FixedString(32)"), ("event_id", "Nullable(FixedString(32))"),
    ("class", "Nullable(String)"), ("url", "Nullable(String)"),
    ("clock", "Nullable(String)"), ("vantage", "Nullable(String)"),
    ("software", "Nullable(String)"), ("method_id", OPT_REF),
    ("public_key", "Nullable(FixedString(32))"), ("route_class", "Nullable(String)"),
    ("software_version", "Nullable(String)"), ("label", "Nullable(String)"),
    ("access_visibility_scope", OPT_REF), ("input", "Nullable(String)"),
    ("algorithm", "Nullable(String)"), ("capability", "Nullable(String)"),
    ("output_unit", "Nullable(String)"), ("failure_taxonomy", "Nullable(String)"),
    ("algorithm_version", "Nullable(UInt16)"), ("protocol_revision", "Nullable(String)"),
    ("format", "Nullable(String)"), ("format_version", "Nullable(UInt16)"),
    ("source_dataset_id", "Nullable(FixedString(16))"),
    ("source_artifact_digest", "Nullable(FixedString(32))"),
]
EVENT = [("event_id", "FixedString(32)"), ("pubkey", "FixedString(32)"),
         ("created_at", "UInt64"), ("kind", "UInt16"), ("tags", "Array(Array(String))"),
         ("content", "String"), ("event_ordinal", REF), ("body_digest", "FixedString(32)")]
ELIGIBILITY = [("event_id", "FixedString(32)"), ("body_digest", "FixedString(32)"),
               ("sig", "FixedString(64)"), ("first_eligible_at", "Int64"),
               ("origin", "String"), ("import_lineage", OPT_REF)]
RECEIVE = [("event_id", "FixedString(32)"), ("received_at", "Int64"),
           ("observer_id", REF), ("clock_id", REF), ("resolution_us", "UInt64"),
           ("uncertainty_us", "Nullable(UInt64)"), ("anomaly_flags", "UInt32"),
           ("corrects", f"Array({SOURCE})")]
INDEX = [("event_id", "FixedString(32)"), ("pubkey", "FixedString(32)"),
         ("created_at", "UInt64"), ("kind", "UInt16"), ("body_digest", "FixedString(32)")]
TAG = [("event_id", "FixedString(32)"), ("tag_key", "String"), ("tag_value", "String")]
PROVENANCE = [("event_id", "FixedString(32)"), ("endpoint_id", REF),
              ("observer_id", REF), ("access_scope_id", REF), ("received_at", "Int64"),
              ("clock_id", REF), ("resolution_us", "UInt64"),
              ("uncertainty_us", "Nullable(UInt64)"), ("anomaly_flags", "UInt32"),
              ("verification_method_id", REF), ("corrects", f"Array({SOURCE})")]
FAMILIES = [
    ("dictionaries", DICTIONARY, "dictionary_ref"),
    ("events", EVENT, "created_at, event_id"),
    ("event_eligibility_facts", ELIGIBILITY, "event_id, visibility_scope"),
    ("event_receive_facts", RECEIVE, "event_id, visibility_scope"),
    ("event_id_index", INDEX, "event_id"),
    ("event_author_index", INDEX, "pubkey, created_at, event_id"),
    ("event_time_index", INDEX, "created_at, event_id"),
    ("event_tag_index", TAG, "tag_key, tag_value, event_id"),
    ("provenance_by_event", PROVENANCE, "event_id, endpoint_id, observer_id, access_scope_id, visibility_scope"),
    ("provenance_by_relay", PROVENANCE, "endpoint_id, event_id, observer_id, access_scope_id, visibility_scope"),
]


def literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def inventory():
    tables = []
    for i, (name, columns, order) in enumerate(FAMILIES):
        key = f"projection_generation, {order}, source, discriminator"
        tables.append(dict(name=name, uuid=str(uuid.UUID(bytes=bytes([i + 1]) * 16)),
                           columns=COMMON + columns, key=key,
                           engine_full=f"MergeTree ORDER BY ({key}) SETTINGS {SETTINGS}"))
    assert [len(t["columns"]) for t in tables] == [35, 15, 13, 15, 12, 12, 12, 10, 18, 18]
    # Independent literal schema inventory must retain the existing identity vector.
    h = hashlib.sha256(b"bigbrotr/index-table-manifest/1\0")
    def text(value):
        raw = value.encode(); h.update(len(raw).to_bytes(4, "big")); h.update(raw)
    for value in [VERSION, DB, "Atomic"]:
        text(value)
    for t in tables:
        text(t["name"]); h.update(uuid.UUID(t["uuid"]).bytes); text(t["engine_full"])
        h.update(len(t["columns"]).to_bytes(2, "big"))
        for name, ty in t["columns"]:
            text(name); text(ty)
    assert h.hexdigest() == "ad1b0478a2f12cac320ffc2b92723cd4cd1f019d2661fddfa2deb1a7af813106"
    return tables


class Lab:
    def __init__(self, args):
        self.root = args.lab_root.resolve()
        self.retained = args.complete_retained_tail
        self.phase = "tail-" if self.retained else ""
        if self.retained:
            assert self.root.is_dir() and self.root.stat().st_mode & 0o777 == 0o700
            assert not (self.root/"tail-result.json").exists(), "continuation already recorded; preserve it"
        else:
            self.root.mkdir(mode=0o700, parents=False, exist_ok=False)
        self.name = args.container
        self.started = False
        self.seq = max(int(p.name.split("-", 1)[0]) for p in (self.root/"results").glob("*.json")) if self.retained else 0
        self.observations = []
        self.checks = []
        self.sources = {}
        for name, path, digest in [("schema.rs", args.schema_source, SCHEMA_SHA),
                                   ("inspect.rs", args.inspect_source, INSPECT_SHA)]:
            data = path.read_bytes()
            assert hashlib.sha256(data).hexdigest() == digest, f"unreviewed {name}"
            if self.retained:
                assert (self.root / name).read_bytes() == data
            else:
                (self.root / name).write_bytes(data)
            self.sources[name] = digest
        if not self.retained:
            for name in ["data", "logs", "private", "results"]:
                (self.root / name).mkdir(mode=0o700)

    def save(self, name, data):
        (self.root / name).write_text(json.dumps(data, indent=2) + "\n")

    def verify(self, condition, name):
        self.checks.append(dict(name=name, status="PASS" if condition else "FAIL"))

    def bound(self):
        used = sum(p.stat().st_size for directory in ["data", "logs"]
                   for p in (self.root / directory).rglob("*") if p.is_file())
        free = shutil.disk_usage(self.root).free
        self.observations.append(dict(time=time.time(), disk_bytes=used, free_bytes=free))
        assert used < 2 * 2**30 and free > 8 * 2**30, "disk budget"

    def command(self, argv, *, data=None, timeout=30):
        return subprocess.run(argv, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=timeout, check=False)

    def query(self, sql, role="initializer", *, rows=False, error=None, name=None):
        self.bound()
        self.seq += 1
        label = f"results/{self.seq:03d}-{name or role}"
        if rows:
            sql += " FORMAT JSONEachRow"
        result = self.command(["docker", "exec", "-i", self.name, "clickhouse-client",
                               "--config-file", f"/lab-private/{role}.xml", "--output_format_json_quote_64bit_integers=0", "--multiquery"],
                              data=sql.encode())
        # Credential-bearing setup is never copied into the ordinary query log.
        if name != "create-writer":
            (self.root / (label + ".sql")).write_text(sql + "\n")
        (self.root / (label + ".stdout")).write_bytes(result.stdout)
        (self.root / (label + ".stderr")).write_bytes(result.stderr)
        self.save(label + ".json", dict(exit_code=result.returncode, expected_error=error))
        if error is not None:
            assert result.returncode != 0 and error.encode() in result.stderr, label
            self.checks.append(dict(name=name, status="PASS", refusal=error))
            return None
        assert result.returncode == 0, label
        return [json.loads(line) for line in result.stdout.splitlines()] if rows else result.stdout.decode().strip()

    def start(self):
        self.bound()
        existing = self.command(["docker", "ps", "-a", "--format", "{{json .}}"])
        assert existing.returncode == 0
        (self.root / "containers-before.jsonl").write_bytes(existing.stdout)
        assert not any(json.loads(line)["Names"] == self.name for line in existing.stdout.splitlines())
        image = self.command(["docker", "image", "inspect", IMAGE])
        assert image.returncode == 0, "pull the exact documented image first"
        info = json.loads(image.stdout)[0]
        assert info["Architecture"] == "amd64" and info["Os"] == "linux"
        assert "clickhouse/clickhouse-server@" + IMAGE.split("@", 1)[1] in info["RepoDigests"]
        self.save("image.json", info)
        passwords = {role: secrets.token_hex(32) for role in ["initializer", "writer"]}
        for role, password in passwords.items():
            (self.root / "private" / f"{role}.xml").write_text(
                f"<config><host>127.0.0.1</host><port>9000</port><user>{role}</user>"
                f"<password>{password}</password></config>\n")
        self.writer_password = passwords["writer"]
        digest = hashlib.sha256(passwords["initializer"].encode()).hexdigest()
        (self.root / "private/users.xml").write_text(
            "<clickhouse><profiles><default><max_memory_usage>268435456</max_memory_usage>"
            "<max_threads>2</max_threads><log_queries>0</log_queries></default></profiles>"
            "<users><initializer><password_sha256_hex>" + digest + "</password_sha256_hex>"
            "<networks><ip>127.0.0.1</ip><ip>::1</ip></networks><profile>default</profile>"
            "<quota>default</quota><access_management>1</access_management></initializer></users>"
            "<quotas><default><interval><duration>3600</duration><queries>0</queries>"
            "<errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows>"
            "<execution_time>0</execution_time></interval></default></quotas></clickhouse>\n")
        (self.root / "private/server.xml").write_text("""<clickhouse>
  <logger><level>warning</level><log>/var/log/clickhouse-server/server.log</log><errorlog>/var/log/clickhouse-server/error.log</errorlog><size>10M</size><count>2</count><console>true</console></logger>
  <path>/var/lib/clickhouse/</path><tmp_path>/var/lib/clickhouse/tmp/</tmp_path><user_files_path>/var/lib/clickhouse/user_files/</user_files_path><format_schema_path>/var/lib/clickhouse/format_schemas/</format_schema_path>
  <users_config>/lab-private/users.xml</users_config><access_control_path>/var/lib/clickhouse/access/</access_control_path><default_profile>default</default_profile><default_database>default</default_database><timezone>UTC</timezone>
  <listen_host>127.0.0.1</listen_host><tcp_port>9000</tcp_port><http_port>8123</http_port><max_connections>16</max_connections><max_concurrent_queries>16</max_concurrent_queries><max_server_memory_usage>3221225472</max_server_memory_usage>
  <max_thread_pool_size>128</max_thread_pool_size><background_schedule_pool_size>16</background_schedule_pool_size><background_pool_size>16</background_pool_size><background_move_pool_size>2</background_move_pool_size><background_fetches_pool_size>2</background_fetches_pool_size><background_common_pool_size>2</background_common_pool_size><background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size><background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size><background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
  <mark_cache_size>67108864</mark_cache_size><uncompressed_cache_size>16777216</uncompressed_cache_size>
</clickhouse>
""")
        argv = ["docker", "run", "-d", "--name", self.name, "--network", "none",
                "--memory", "4g", "--memory-swap", "4g", "--cpus", "2", "--pids-limit", "256",
                "--user", f"{os.getuid()}:{os.getgid()}", "--cap-drop", "ALL",
                "--security-opt", "no-new-privileges", "--read-only", "--tmpfs", "/tmp:rw,size=64m",
                "--ulimit", "nofile=65536:65536", "--log-opt", "max-size=10m", "--log-opt", "max-file=2",
                "--label", "org.bigbrotr.research=initial-index-schema-20261026",
                "--mount", f"type=bind,source={self.root / 'data'},target=/var/lib/clickhouse",
                "--mount", f"type=bind,source={self.root / 'logs'},target=/var/log/clickhouse-server",
                "--mount", f"type=bind,source={self.root / 'private'},target=/lab-private,readonly",
                "--entrypoint", "/usr/bin/clickhouse-server", IMAGE, "--config-file=/lab-private/server.xml"]
        self.save("start-command.json", argv)
        result = self.command(argv)
        (self.root / "start.stderr").write_bytes(result.stderr)
        assert result.returncode == 0, "server start"
        self.started = True
        self.save("container-id.json", result.stdout.decode().strip())
        for _ in range(30):
            self.bound()
            result = self.command(["docker", "exec", self.name, "clickhouse-client",
                                   "--config-file", "/lab-private/initializer.xml", "--query", "SELECT 1"])
            if result.returncode == 0:
                break
            (self.root / "startup-last.stderr").write_bytes(result.stderr)
            time.sleep(1)
        else:
            raise AssertionError("server never became ready")
        inspection = json.loads(self.command(["docker", "inspect", self.name]).stdout)[0]
        self.save("container-active.json", inspection)
        host = inspection["HostConfig"]
        assert host["Memory"] == host["MemorySwap"] == 4 * 2**30
        assert host["NanoCpus"] == 2_000_000_000 and host["PidsLimit"] == 256
        assert host["NetworkMode"] == "none" and not host["PortBindings"]
        stats = self.command(["docker", "stats", "--no-stream", "--format", "{{json .}}", self.name])
        (self.root / "active-stats.jsonl").write_bytes(stats.stdout)

    def resume_tail(self):
        # A single verifier-failure continuation, not adoption of another lab.
        # Compare the corrected oracle to retained facts before any engine start.
        old_result = json.loads((self.root/"result.json").read_text())
        assert old_result["status"] == "FAIL" and old_result["error"] == "AssertionError()"
        assert [c["name"] for c in old_result["checks"] if c["status"] == "FAIL"] == [
            "ten table UUID engines keys settings match frozen adapter",
            "all160 columns types positions and defaults match frozen adapter",
        ]
        def capture(label):
            files = list((self.root/"results").glob("*-" + label + ".stdout"))
            assert len(files) == 1
            return [json.loads(line) for line in files[0].read_text().splitlines()]
        tables = inventory()
        expected_tables = [dict(name=t["name"],uuid=t["uuid"],engine="MergeTree",engine_full=t["engine_full"],partition_key="",sorting_key=t["key"],primary_key=t["key"],sampling_key="",storage_policy="default",dependent_databases=0,dependent_tables=0) for t in sorted(tables,key=lambda t:t["name"])]
        expected_columns = [dict(table=t["name"],name=name,type=ty,position=i+1,default_kind="",default_expression="") for t in sorted(tables,key=lambda t:t["name"]) for i,(name,ty) in enumerate(t["columns"])]
        assert capture("table-facts") == expected_tables
        assert capture("column-facts") == expected_columns
        sql = f"SELECT created_at,tags,content FROM {DB}.events WHERE discriminator=2 FORMAT JSONEachRow"
        files = [p for p in (self.root/"results").glob("*.sql") if p.read_text().strip() == sql]
        assert len(files) == 1
        assert json.loads(files[0].with_suffix(".stdout").read_text()) == dict(created_at=18446744073709551615,tags=[["p","alpha","tail"],[],["t","é"]],content="typed SQL fixture")
        self.save("retained-json-oracle.json", dict(status="PASS", original_result_sha256=hashlib.sha256((self.root/"result.json").read_bytes()).hexdigest(), scope="same exact literal values and inventories; JSON integers; original FAIL unchanged"))
        self.checks.append(dict(name="corrected oracle passes retained ten-table160-column and UInt64max captures",status="PASS"))
        self.bound()
        old = json.loads((self.root/"container-final.json").read_text())[0]
        now = json.loads(self.command(["docker","inspect",self.name]).stdout)[0]
        assert now["Id"] == old["Id"] and not now["State"]["Running"] and now["State"]["Pid"] == 0
        assert now["Config"]["Image"] == IMAGE
        assert now["Config"]["Labels"]["org.bigbrotr.research"] == "initial-index-schema-20261026"
        assert {m["Destination"]:m["Source"] for m in now["Mounts"]} == {
            "/var/lib/clickhouse":str(self.root/"data"),
            "/var/log/clickhouse-server":str(self.root/"logs"),
            "/lab-private":str(self.root/"private"),
        }
        host = now["HostConfig"]
        assert host["Memory"] == host["MemorySwap"] == 4*2**30
        assert host["NanoCpus"] == 2_000_000_000 and host["PidsLimit"] == 256
        assert host["NetworkMode"] == "none" and not host["PortBindings"]
        self.save("tail-before-start.json", now)
        result = self.command(["docker","start",self.name])
        assert result.returncode == 0
        self.started = True
        for _ in range(30):
            result = self.command(["docker","exec",self.name,"clickhouse-client","--config-file","/lab-private/initializer.xml","--query","SELECT 1"])
            if result.returncode == 0:
                return
            self.bound()
            time.sleep(1)
        raise AssertionError("retained server never became ready")

    def qualify(self):
        tables = inventory()
        self.save("expected-tables.json", tables)
        assert self.query("SELECT version() AS version, currentUser() AS user", rows=True) == [dict(version=VERSION, user="initializer")]
        self.query("SELECT name,value FROM system.build_options WHERE name LIKE '%GIT%' OR name LIKE '%VERSION%' ORDER BY name", rows=True, name="build-identity")
        self.query(f"CREATE DATABASE {DB} ENGINE = Atomic")
        assert self.query(f"SELECT engine FROM system.databases WHERE name='{DB}'", rows=True) == [dict(engine="Atomic")]
        assert self.query(f"SELECT count() FROM system.tables WHERE database='{DB}'") == "0"
        assert self.query("SELECT name,type FROM system.disks WHERE name='default'", rows=True) == [dict(name="default", type="Local")]
        assert self.query("SELECT toUInt8(length(disks)=1 AND disks[1]='default') AS local_only FROM system.storage_policies WHERE policy_name='default'", rows=True) == [dict(local_only=1)]
        self.checks.append(dict(name="exact server version fresh Atomic database and local default policy", status="PASS"))
        self.query(f"CREATE USER writer IDENTIFIED WITH sha256_password BY '{self.writer_password}' HOST LOCAL SETTINGS max_concurrent_queries_for_user = 1 MIN 1 MAX 1", name="create-writer")
        del self.writer_password
        for t in tables:
            fields = ", ".join(f"`{n}` {ty}" for n, ty in t["columns"])
            self.query(f"CREATE TABLE `{DB}`.`{t['name']}` UUID '{t['uuid']}' ({fields}) ENGINE = {t['engine_full']}")
            self.query(f"GRANT SELECT, INSERT ON `{DB}`.`{t['name']}` TO writer")
        for table in ["databases", "disks", "storage_policies", "settings", "tables", "columns", "projections"]:
            self.query(f"GRANT SELECT ON system.{table} TO writer")
        self.query("SHOW GRANTS FOR writer", name="writer-grants")
        assert self.query("SELECT version() AS version,currentUser() AS user", "writer", rows=True) == [dict(version=VERSION,user="writer")]
        assert self.query(f"SELECT engine FROM system.databases WHERE name='{DB}'", "writer", rows=True) == [dict(engine="Atomic")]
        assert self.query("SELECT name,type FROM system.disks WHERE name='default'", "writer", rows=True) == [dict(name="default",type="Local")]
        assert self.query("SELECT toUInt8(length(disks)=1 AND disks[1]='default') AS local_only FROM system.storage_policies WHERE policy_name='default'", "writer", rows=True) == [dict(local_only=1)]
        self.checks.append(dict(name="writer can execute all engine identity queries under narrow grants", status="PASS"))
        self.query("SELECT name, value, min, max, readonly FROM system.settings WHERE name='max_concurrent_queries_for_user'", "writer", rows=True, name="writer-constraint-facts")
        self.verify(self.query("SELECT value,min,max FROM system.settings WHERE name='max_concurrent_queries_for_user'", "writer", rows=True) == [dict(value="1", min="1", max="1")], "persistent writer min=value=max=1")
        self.query(f"SELECT name,create_table_query FROM system.tables WHERE database='{DB}' ORDER BY name SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1", "writer", rows=True, name="create-definition-facts")
        actual = self.query(f"SELECT name,toString(uuid) AS uuid,engine,engine_full,partition_key,sorting_key,primary_key,sampling_key,storage_policy,length(dependencies_database) AS dependent_databases,length(dependencies_table) AS dependent_tables FROM system.tables WHERE database='{DB}' ORDER BY name", "writer", rows=True, name="table-facts")
        expected = [dict(name=t["name"], uuid=t["uuid"], engine="MergeTree", engine_full=t["engine_full"], partition_key="", sorting_key=t["key"], primary_key=t["key"], sampling_key="", storage_policy="default", dependent_databases=0, dependent_tables=0) for t in sorted(tables, key=lambda t:t["name"])]
        self.save("expected-table-catalog.json", expected)
        self.verify(actual == expected, "ten table UUID engines keys settings match frozen adapter")
        columns = self.query(f"SELECT table,name,type,position,default_kind,default_expression FROM system.columns WHERE database='{DB}' ORDER BY table,position", "writer", rows=True, name="column-facts")
        expected = [dict(table=t["name"], name=name, type=ty, position=i+1, default_kind="", default_expression="") for t in sorted(tables,key=lambda t:t["name"]) for i,(name,ty) in enumerate(t["columns"])]
        self.save("expected-column-catalog.json", expected)
        self.verify(columns == expected, "all160 columns types positions and defaults match frozen adapter")
        self.verify(self.query(f"SELECT count() FROM system.projections WHERE database='{DB}'", "writer") == "0", "no native projections")
        for t in tables:
            assert self.query(f"SELECT count() FROM {DB}.{t['name']}", "writer") == "0"
        self.checks.append(dict(name="all10 installed tables initially empty", status="PASS"))
        # Verify the setting definitions independently, then per-table overrides
        # through the server's normalized engine_full and SHOW CREATE output.
        self.query("SELECT name,value,type,description FROM system.merge_tree_settings WHERE name IN ('fsync_after_insert','fsync_part_directory','non_replicated_deduplication_window') ORDER BY name", rows=True, name="setting-definitions")
        for t in tables:
            self.query(f"SHOW CREATE TABLE {DB}.{t['name']}", name="show-create")
            described = self.query(f"DESCRIBE TABLE {DB}.{t['name']}", "writer", rows=True, name="writer-describe")
            expected_describe = [dict(name=name,type=ty,default_type="",default_expression="",comment="",codec_expression="",ttl_expression="") for name,ty in t["columns"]]
            self.verify(described == expected_describe, f"writer DESCRIBE seven-field metadata {t['name']}")
        definitions = {}
        for t in tables:
            fields = ", ".join(f"`{n}` {ty}" for n, ty in t["columns"])
            ddl = f"CREATE TABLE `{DB}`.`{t['name']}` UUID '{t['uuid']}' ({fields}) ENGINE = {t['engine_full']}"
            self.query(f"SELECT formatQuerySingleLine({literal(ddl)}) AS expected", "writer", rows=True, name="expected-create-definition")
            definitions[t["name"]] = self.definition_matches(t)
        self.verify(all(value == 1 for value in definitions.values()), "all10 complete canonical create definitions match")
        for value in [0, 2]:
            self.query(f"SELECT 1 SETTINGS max_concurrent_queries_for_user={value}", "writer", error="SETTING_CONSTRAINT_VIOLATION", name=f"constraint-refuses-{value}")
        for name, sql in [
            ("create-refused", f"CREATE TABLE {DB}.forbidden (a UInt8) ENGINE=Memory"),
            ("alter-refused", f"ALTER TABLE {DB}.events ADD COLUMN forbidden UInt8"),
            ("truncate-refused", f"TRUNCATE TABLE {DB}.events"),
            ("drop-refused", f"DROP TABLE {DB}.events"),
            ("grant-refused", f"GRANT ALTER ON {DB}.* TO writer"),
            ("alter-user-refused", "ALTER USER writer SETTINGS max_concurrent_queries_for_user=2"),
        ]:
            self.query(sql, "writer", error="ACCESS_DENIED", name=name)
        # Small SQL storage checks, not Fact mapping, signed-event or wire proofs.
        for t in tables:
            self.query(f"INSERT INTO {DB}.{t['name']} (discriminator) VALUES (1)", "writer")
            assert self.query(f"SELECT count() FROM {DB}.{t['name']}", "writer") == "1"
        self.query(f"INSERT INTO {DB}.event_tag_index (discriminator) VALUES (1)", "writer")
        assert self.query(f"SELECT count() FROM {DB}.event_tag_index WHERE discriminator=1", "writer") == "2"
        self.query(f"INSERT INTO {DB}.events (discriminator,created_at,tags,content) VALUES (2,18446744073709551615,[['p','alpha','tail'],[],['t','é']],'typed SQL fixture')", "writer")
        assert self.query(f"SELECT created_at,tags,content FROM {DB}.events WHERE discriminator=2", "writer", rows=True) == [dict(created_at=18446744073709551615, tags=[["p","alpha","tail"],[],["t","é"]], content="typed SQL fixture")]
        self.checks.append(dict(name="narrow writer inserts all10 and UInt64max nested tags roundtrip", status="PASS"))
        self.complete_tail(tables)

    def complete_tail(self, tables):
        assert self.query(f"SELECT toTypeName(length(dependencies_database)) AS databases,toTypeName(length(dependencies_table)) AS tables FROM system.tables WHERE database='{DB}' LIMIT 1", "writer", rows=True) == [dict(databases="UInt64",tables="UInt64")]
        assert self.query(f"SELECT toTypeName(position) AS position FROM system.columns WHERE database='{DB}' LIMIT 1", "writer", rows=True) == [dict(position="UInt64")]
        assert self.query(f"SELECT toTypeName(created_at) AS created_at FROM {DB}.events LIMIT 1", "writer", rows=True) == [dict(created_at="UInt64")]
        assert self.query("SELECT toTypeName(value) AS value,toTypeName(min) AS min,toTypeName(max) AS max FROM system.settings WHERE name='max_concurrent_queries_for_user'", "writer", rows=True) == [dict(value="String",min="Nullable(String)",max="Nullable(String)")]
        self.checks.append(dict(name="actual numeric and optional setting type names match adapter", status="PASS"))
        parts = self.query(f"SELECT table,groupUniqArray(partition_id) AS partition_ids,sum(rows) AS rows FROM system.parts WHERE active AND database='{DB}' GROUP BY table ORDER BY table", rows=True, name="physical-partitions")
        expected_counts = {t["name"]:(2 if t["name"] in ["events","event_tag_index"] else 1) for t in tables}
        self.verify(len(parts) == 10 and all(row["partition_ids"] == ["all"] for row in parts) and {r["table"]:r["rows"] for r in parts} == expected_counts, "all10 physical partition IDs all; duplicate row retained")
        self.concurrency()
        # Same table UUID/types/engine can hide an unselected per-column TTL.
        # Mutation is confined to this final synthetic negative control.
        event = tables[1]
        baseline_matches = self.definition_matches(event)
        self.query(f"ALTER TABLE {DB}.events MODIFY COLUMN content String TTL toDateTime(recorded_at) + INTERVAL 100 YEAR", name="column-ttl-control")
        state = self.query(f"SELECT toString(uuid) AS uuid,engine_full FROM system.tables WHERE database='{DB}' AND name='events'", "writer", rows=True, name="column-ttl-identity")
        assert state == [dict(uuid=event["uuid"], engine_full=event["engine_full"])]
        columns = self.query(f"SELECT name,type,default_kind,default_expression FROM system.columns WHERE database='{DB}' AND table='events' ORDER BY position", "writer", rows=True, name="column-ttl-columns")
        assert columns == [dict(name=name,type=ty,default_kind="",default_expression="") for name,ty in event["columns"]]
        ttl_matches = self.definition_matches(event)
        self.query(f"SHOW CREATE TABLE {DB}.events", name="column-ttl-show-create")
        self.verify(baseline_matches == 1 and ttl_matches == 0, "full definition rejects column TTL with same UUID types and engine")

    def definition_matches(self, table):
        fields = ", ".join(f"`{n}` {ty}" for n, ty in table["columns"])
        expected = f"CREATE TABLE `{DB}`.`{table['name']}` UUID '{table['uuid']}' ({fields}) ENGINE = {table['engine_full']}"
        rows = self.query(f"SELECT toUInt8(formatQuerySingleLine(create_table_query)=formatQuerySingleLine({literal(expected)})) AS matches FROM system.tables WHERE database='{DB}' AND name='{table['name']}' SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1", "writer", rows=True, name="create-definition-equality")
        assert len(rows) == 1
        return rows[0]["matches"]

    def concurrency(self):
        query_id = "initial-index-fixed-user-limit"
        child = subprocess.Popen(["docker", "exec", self.name, "clickhouse-client", "--config-file", "/lab-private/writer.xml", "--query_id", query_id, "--query", "SELECT sleep(3)"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            for _ in range(30):
                if self.query(f"SELECT count() FROM system.processes WHERE query_id='{query_id}'") == "1":
                    break
                time.sleep(0.05)
            else:
                raise AssertionError("no independently observed concurrent writer query")
            self.query("SELECT 1", "writer", error="TOO_MANY_SIMULTANEOUS_QUERIES", name="second-writer-refused")
        finally:
            stdout, stderr = child.communicate(timeout=10)
            (self.root/"concurrent.stdout").write_bytes(stdout)
            (self.root/"concurrent.stderr").write_bytes(stderr)
        assert child.returncode == 0
        assert self.query("SELECT 1", "writer") == "1"
        self.checks.append(dict(name="writer succeeds after finite prior query completes", status="PASS"))

    def stop(self):
        if self.started:
            result = self.command(["docker", "stop", "--time", "30", self.name], timeout=40)
            (self.root/(self.phase + "stop.stdout")).write_bytes(result.stdout)
            (self.root/(self.phase + "stop.stderr")).write_bytes(result.stderr)
            logs = self.command(["docker", "logs", self.name])
            (self.root/(self.phase + "docker.stdout")).write_bytes(logs.stdout)
            (self.root/(self.phase + "docker.stderr")).write_bytes(logs.stderr)
            inspect = self.command(["docker", "inspect", self.name])
            self.save(self.phase + "container-final.json", json.loads(inspect.stdout))
            state = json.loads(inspect.stdout)[0]["State"]
            assert result.returncode == 0 and not state["Running"] and state["Pid"] == 0
            assert state["ExitCode"] == 0 and not state["OOMKilled"]
        self.bound()
        self.save(self.phase + "resource-samples.json", self.observations)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab-root", type=Path, required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--schema-source", type=Path, required=True)
    parser.add_argument("--inspect-source", type=Path, required=True)
    parser.add_argument("--complete-retained-tail", action="store_true", help="complete only the captured JSON-oracle failure's missing controls")
    args = parser.parse_args()
    assert args.container.startswith("bigbrotr-index-schema-")
    os.umask(0o077)
    lab = Lab(args)
    result = dict(scope="SQL/native-client only; HTTP/Rust adapter/PG/product NOT_RUN", status="FAIL")
    try:
        if lab.retained:
            lab.resume_tail()
            lab.complete_tail(inventory())
        else:
            lab.start()
            lab.qualify()
        result["status"] = "FAIL" if any(check["status"] == "FAIL" for check in lab.checks) else "PASS_SCOPED"
    except Exception as exc:
        result["error"] = repr(exc)
        (lab.root/(lab.phase + "failure-traceback.txt")).write_text(traceback.format_exc())
    finally:
        try:
            lab.stop()
        except Exception as exc:
            result["stop_error"] = repr(exc)
            result["status"] = "FAIL"
        result.update(checks=lab.checks, sources=lab.sources)
        lab.save(lab.phase + "result.json", result)
    print(json.dumps({"status":result["status"], "lab_root":str(lab.root)}))
    return 0 if result["status"] == "PASS_SCOPED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
