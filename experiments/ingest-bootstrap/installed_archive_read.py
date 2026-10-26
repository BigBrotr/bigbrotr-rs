#!/usr/bin/env python3
"""Independent read-only C-API census after the installed owner has stopped.

Uses the already authenticated/pinned library and extensions, not product codecs
or queries. No expected rows are passed to this reader. Secret-bearing errors are
withheld; output consists only of explicit scientific columns and commit rows.
"""
import argparse
import ctypes as C
import json
from pathlib import Path
import re
import sys
import tomllib


class Result(C.Structure):
    _fields_ = [('deprecated_column_count', C.c_uint64), ('deprecated_row_count', C.c_uint64),
        ('deprecated_rows_changed', C.c_uint64), ('deprecated_columns', C.c_void_p),
        ('deprecated_error_message', C.c_void_p), ('internal_data', C.c_void_p)]


def decode_row(raw, row_bytes):
    """Same strict census byte check, independently exercisable without an engine."""
    assert row_bytes in (65536, 262144, 1048576)
    assert len(raw) < row_bytes, 'native-reader-row-byte-bound'
    return json.loads(raw)


def catalog_settings(config, catalog_port):
    """An explicit loopback port changes placement, never the allowed host."""
    assert type(catalog_port) is int and 1 <= catalog_port <= 65535
    pg = config['postgres']['catalog']
    assert pg['endpoint'] == f'127.0.0.1:{catalog_port}'
    assert all(re.fullmatch('[a-z0-9_]+', pg[k]) for k in ('database', 'read_user'))
    return pg


def read(config, temporary, *, full_stream_order=False, row_bytes=65536,
         bundle_root=Path('/build/archive-worker'), catalog_port=5432):
    assert row_bytes in (65536, 262144, 1048576)
    pg = catalog_settings(config, catalog_port)
    bundle = Path(bundle_root)
    assert bundle.is_absolute()
    header = (bundle / 'lib/duckdb.h').read_text()
    declaration = re.search(r'typedef struct \{([^{}]+)\}\s*duckdb_result;', header)
    assert declaration is not None
    fields = re.sub(r'/\*.*?\*/|//[^\n]*', '', declaration[1], flags=re.S)
    assert re.sub(r'\s+', '', fields) == ('idx_tdeprecated_column_count;idx_tdeprecated_row_count;'
        'idx_tdeprecated_rows_changed;duckdb_column*deprecated_columns;char*deprecated_error_message;void*internal_data;')
    engine = C.CDLL(str(bundle / 'lib/libduckdb.so'))
    def bind(name, result, args):
        call = getattr(engine, name); call.restype = result; call.argtypes = args
        return call
    void, handle = C.c_void_p, C.POINTER(C.c_void_p)
    version = bind('duckdb_library_version', C.c_char_p, [])
    assert version() == b'v1.5.5'
    create_config = bind('duckdb_create_config', C.c_int, [handle])
    set_config = bind('duckdb_set_config', C.c_int, [void, C.c_char_p, C.c_char_p])
    destroy_config = bind('duckdb_destroy_config', None, [handle])
    open_db = bind('duckdb_open_ext', C.c_int, [C.c_char_p, handle, void, handle])
    connect = bind('duckdb_connect', C.c_int, [void, handle])
    query = bind('duckdb_query', C.c_int, [void, C.c_char_p, C.POINTER(Result)])
    destroy_result = bind('duckdb_destroy_result', None, [C.POINTER(Result)])
    count = bind('duckdb_row_count', C.c_uint64, [C.POINTER(Result)])
    column_count = bind('duckdb_column_count', C.c_uint64, [C.POINTER(Result)])
    varchar = bind('duckdb_value_varchar', void, [C.POINTER(Result), C.c_uint64, C.c_uint64])
    free = bind('duckdb_free', None, [void])
    disconnect = bind('duckdb_disconnect', None, [handle])
    close = bind('duckdb_close', None, [handle])
    configuration, database, connection, error = void(), void(), void(), void()
    assert create_config(C.byref(configuration)) == 0
    try:
        options = dict(threads='1', memory_limit='128MiB', temp_directory=str(temporary),
            max_temp_directory_size='64MiB', autoload_known_extensions='false',
            autoinstall_known_extensions='false', allow_persistent_secrets='false',
            allow_unsigned_extensions='false', allow_community_extensions='false')
        for key, value in options.items():
            assert set_config(configuration, key.encode(), value.encode()) == 0, 'native-reader-config'
        assert open_db(None, C.byref(database), configuration, C.byref(error)) == 0, 'native-reader-open'
        assert connect(database, C.byref(connection)) == 0, 'native-reader-connect'
        def execute(sql, label, maximum=0):
            result = Result()
            try:
                assert query(connection, sql.encode(), C.byref(result)) == 0, 'native-reader-' + label
                if maximum == 0:
                    return None
                length = count(C.byref(result))
                assert column_count(C.byref(result)) == 1 and length <= maximum, 'native-reader-row-bound'
                values = []
                for index in range(length):
                    value = varchar(C.byref(result), 0, index)
                    assert value
                    try:
                        raw = C.string_at(value)
                        values.append(decode_row(raw, row_bytes))
                    finally:
                        free(value)
                return values
            finally:
                destroy_result(C.byref(result))
        for name in ('httpfs', 'postgres_scanner', 'ducklake'):
            execute(f"LOAD '{bundle}/extensions/{name}.duckdb_extension'", 'extension-' + name)
        execute("SET pg_pool_enable_reaper_thread=false; SET pg_use_ctid_scan=false; SET pg_pool_max_connections=2; SET pg_pool_acquire_mode='try'", 'pg-settings')
        secret = Path(pg['read_password_ref'].removeprefix('file:')).read_text()
        assert re.fullmatch('[0-9a-f]{64}', secret)
        execute("CREATE TEMPORARY SECRET reader (TYPE postgres, URI 'hostaddr=127.0.0.1 port="
            + str(catalog_port) + ' dbname='
            + pg['database'] + ' user=' + pg['read_user'] + ' password=' + secret
            + " sslmode=disable connect_timeout=5')", 'credential')
        del secret
        root = config['storage']['archive_root']
        assert re.fullmatch('/[a-zA-Z0-9/_-]+', root)
        execute("ATTACH 'ducklake:postgres:' AS archive (DATA_PATH '" + root
            + "/', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION false, CREATE_IF_NOT_EXISTS false, META_SECRET_STORAGE_TABLE '', META_SECRET 'reader', READ_ONLY)", 'attach')
        execute('BEGIN TRANSACTION', 'begin')
        execute("CREATE MACRO ref_value(r) AS CASE WHEN r IS NULL THEN NULL ELSE struct_pack(dataset_id:=hex(r.dataset_id),dictionary_kind:=r.dictionary_kind,allocation_domain:=hex(r.allocation_domain),ordinal:=r.ordinal) END; CREATE MACRO source_value(r) AS CASE WHEN r IS NULL THEN NULL ELSE struct_pack(dataset_id:=hex(r.dataset_id),shard_id:=r.shard_id,owner_epoch:=r.owner_epoch,sequence:=r.sequence) END; CREATE MACRO clock_value(r) AS struct_pack(clock_id:=ref_value(r.clock_id),resolution_us:=r.resolution_us,uncertainty_us:=r.uncertainty_us,anomaly_flags:=r.anomaly_flags)", 'macros')
        common = 'source_value(source_record_id) AS source_record_id,record_schema_version,record_kind,ref_value(visibility_scope_id) AS visibility_scope_id,hex(payload_digest) AS payload_digest,recorded_at'
        descriptor_fields = 'event_id class url clock vantage software method_id public_key route_class software_version label visibility_scope_id input algorithm capability output_unit failure_taxonomy algorithm_version protocol_revision format format_version source_dataset_id source_artifact_digest'.split()
        descriptor = []
        for field in descriptor_fields:
            value = 'descriptor_value.' + field
            if field in ('method_id', 'visibility_scope_id'): value = 'ref_value(' + value + ')'
            if field in ('event_id', 'public_key', 'source_dataset_id', 'source_artifact_digest'): value = 'hex(' + value + ')'
            descriptor.append(field + ':=' + value)
        families = dict(
            dictionaries='ref_value(reference) AS reference,normalization_version,descriptor_schema,descriptor_version,struct_pack(' + ','.join(descriptor) + ') AS descriptor_value',
            events='hex(event_id) AS event_id,hex(pubkey) AS pubkey,created_at,kind,tags,content,ref_value(event_ordinal) AS event_ordinal',
            event_eligibility_facts='hex(event_id) AS event_id,hex(body_digest) AS body_digest,hex(sig) AS sig,first_eligible_at,origin,ref_value(import_lineage) AS import_lineage',
            event_receive_facts='hex(event_id) AS event_id,received_at,ref_value(observer_id) AS observer_id,clock_value(clock_quality) AS clock_quality,source_value(corrects) AS corrects',
            provenance_facts='hex(event_id) AS event_id,ref_value(endpoint_id) AS endpoint_id,ref_value(observer_id) AS observer_id,ref_value(access_scope_id) AS access_scope_id,received_at,clock_value(clock_quality) AS clock_quality,ref_value(verification_method_id) AS verification_method_id,source_value(corrects) AS corrects')
        order = ('source_record_id.dataset_id,source_record_id.shard_id,source_record_id.owner_epoch,source_record_id.sequence'
            if full_stream_order else 'source_record_id.sequence')
        tables = {name: execute(f'SELECT to_json(r) FROM (SELECT {common},{fields} FROM archive.{name} ORDER BY {order} LIMIT 17) r', name, 16)
            for name, fields in families.items()}
        tables['archive_commits'] = execute('SELECT to_json(r) FROM (SELECT hex(dataset_id) AS dataset_id,schema_version,topology_generation,hex(batch_digest) AS batch_digest,record_count,input_ranges,hex(attempt_id) AS attempt_id,publication_ordinal FROM archive.archive_commits ORDER BY publication_ordinal LIMIT 9) r', 'commits', 8)
        execute('ROLLBACK', 'rollback')
        return dict(format='bigbrotr-independent-installed-archive-read/1', tables=tables)
    finally:
        if connection: disconnect(C.byref(connection))
        if database: close(C.byref(database))
        if configuration: destroy_config(C.byref(configuration))
        if error: free(error)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', type=Path)
    parser.add_argument('temporary', type=Path)
    parser.add_argument('--full-stream-order', action='store_true')
    row_limit = parser.add_mutually_exclusive_group()
    row_limit.add_argument('--large-signed-row', action='store_true')
    row_limit.add_argument('--row-bytes', type=int, choices=(65536, 262144, 1048576),
        help='explicit independent census envelope; no production row-limit change')
    parser.add_argument('--worker-root', type=Path, default=Path('/build/archive-worker'))
    parser.add_argument('--catalog-port', type=int, default=5432,
        help='explicit 127.0.0.1 catalog port; must match the configured endpoint')
    args = parser.parse_args()
    config, temporary = args.config, args.temporary
    assert config.is_absolute() and temporary.is_absolute()
    temporary.mkdir(mode=0o700)
    print(json.dumps(read(tomllib.loads(config.read_text()), temporary, full_stream_order=args.full_stream_order,
        row_bytes=args.row_bytes or (262144 if args.large_signed_row else 65536), bundle_root=args.worker_root,
        catalog_port=args.catalog_port)))
