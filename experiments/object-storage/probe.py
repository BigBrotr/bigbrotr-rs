"""Bounded real DuckLake/PG/S3 experiment; not an archive implementation."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

import boto3
from botocore.config import Config
import duckdb
import psycopg2
from coincurve import PublicKeyXOnly

ROOT = Path('/lab')
ENDPOINT = 'http://127.0.0.1:48700'
REPORT = ROOT / 'reports/object-storage.json'


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def save(report):
    REPORT.with_suffix('.pending').write_text(json.dumps(report, indent=2) + '\n')
    REPORT.with_suffix('.pending').replace(REPORT)


def client(credentials):
    return boto3.client('s3', endpoint_url=ENDPOINT, region_name='garage',
                        aws_access_key_id=credentials['key'],
                        aws_secret_access_key=credentials['secret'],
                        config=Config(signature_version='s3v4',
                                      s3={'addressing_style': 'path'},
                                      retries={'max_attempts': 2}))


def connect(database, path, credentials, readonly=False, user='postgres', extensions='/lab/extensions', override=False):
    con = duckdb.connect(config={'threads': '4', 'memory_limit': '6GB',
                                 'extension_directory': extensions,
                                 'temp_directory': '/tmp/object-storage-spill',
                                 'max_temp_directory_size': '16GB'})
    for extension in ('ducklake', 'postgres', 'httpfs'):
        if not readonly:
            con.execute('INSTALL ' + extension)
        con.execute('LOAD ' + extension)
    con.execute("CREATE SECRET s3_lab (TYPE s3, PROVIDER config, KEY_ID " + literal(credentials['key']) +
                ', SECRET ' + literal(credentials['secret']) +
                ", REGION 'garage', ENDPOINT '127.0.0.1:48700', URL_STYLE 'path', USE_SSL false)")
    con.execute(f"ATTACH 'ducklake:postgres:dbname={database} host=127.0.0.1 port=48732 user={user}' AS lake "
                f"(DATA_PATH {literal(path)}, DATA_INLINING_ROW_LIMIT 0" +
                (', READ_ONLY' if readonly else '') + (', OVERRIDE_DATA_PATH true' if override else '') + ')')
    return con


def verify(con, expected, rows):
    started = time.monotonic()
    population = list(map(int, con.execute('SELECT count(*),count(DISTINCT id),'
        'count(DISTINCT ordinal),sum(ordinal),min(ordinal),max(ordinal) FROM lake.events').fetchone()))
    assert population == [rows, rows, rows, rows * (rows - 1) // 2, 0, rows - 1], population
    keys = ','.join(str(row['ordinal']) for row in expected)
    actual = con.execute('SELECT ordinal,lower(hex(id)),lower(hex(pubkey)),created_at,kind,tags,content,'
                         f'lower(hex(sig)) FROM lake.events WHERE ordinal IN ({keys}) ORDER BY ordinal').fetchall()
    assert len(actual) == len(expected)
    names = ('ordinal', 'id', 'pubkey', 'created_at', 'kind', 'tags', 'content', 'sig')
    result = []
    for fields, original in zip(actual, expected):
        row = dict(zip(names, fields))
        assert row == original, 'Full signed-field mismatch'
        canonical = json.dumps([0, row['pubkey'], row['created_at'], row['kind'], row['tags'],
                                row['content']], ensure_ascii=False, separators=(',', ':')).encode()
        digest = hashlib.sha256(canonical).digest()
        assert digest.hex() == row['id']
        assert PublicKeyXOnly(bytes.fromhex(row['pubkey'])).verify(bytes.fromhex(row['sig']), digest)
        result.append(row)
    return {'population': population, 'signed_samples': len(result),
            'sample_sha256': hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False,
                                              separators=(',', ':')).encode()).hexdigest(),
            'seconds': time.monotonic() - started}


def expected_rows(count):
    ordinals = sorted({0, count - 1, 4095, 4096} | {
        int.from_bytes(hashlib.sha256(f'object-storage-v1:{i}'.encode()).digest()[:8], 'big') % count
        for i in range(128)})
    generator = '/baseline/src/storage-qualification/fixture/target/release/bigbrotr-research-fixture'
    result = subprocess.run([generator, 'list', '8192', '1'], input=json.dumps(ordinals),
                            text=True, capture_output=True, check=True)
    return [json.loads(line) for line in result.stdout.splitlines()]


def main():
    assert not REPORT.exists(), 'Do not overwrite evidence; assign another run identity'
    credentials = json.loads((ROOT / 'credentials.json').read_text())
    s3 = client(credentials)
    sources = sorted(Path('/baseline/input/aged').glob('*.parquet'))[:16]
    assert len(sources) == 16
    count = 16 * 65536
    expected = expected_rows(count)
    (ROOT / 'expected.json').write_text(json.dumps({'rows': count, 'events': expected}))
    report = {'status': 'RUNNING', 'rows': count, 'duckdb': duckdb.__version__,
              'cache': 'uncontrolled shared host caches; not cold-cache or WAN throughput',
              'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'source_files': [{'name': p.name, 'bytes': p.stat().st_size,
                                'sha256': hashlib.file_digest(p.open('rb'), 'sha256').hexdigest()}
                               for p in sources], 'layouts': {}}
    save(report)
    pg = psycopg2.connect('dbname=qualification host=127.0.0.1 port=48732 user=postgres')
    pg.autocommit = True
    with pg.cursor() as cur:
        cur.execute('CREATE DATABASE catalog_local')
        cur.execute('CREATE DATABASE catalog_s3')
    for name, path in [('local', '/lab/local/archive'), ('s3', 's3://qualification/archive/')]:
        con = connect('catalog_' + name, path, credentials)
        con.execute(f'CREATE TABLE lake.events AS SELECT * FROM read_parquet({literal(sources[0])}) LIMIT 0')
        con.execute('CREATE TABLE lake.receipts(batch INTEGER, rows BIGINT)')
        for key, value in [('parquet_compression', 'zstd'), ('parquet_compression_level', '3'),
                           ('parquet_row_group_size', '8192'), ('target_file_size', '256MB')]:
            con.execute(f'CALL lake.set_option({literal(key)}, {literal(value)}, table_name=>\'events\')')
        batches = []
        for offset in range(0, len(sources), 4):
            started = time.monotonic()
            paths = ','.join(literal(p) for p in sources[offset:offset+4])
            con.execute('BEGIN')
            con.execute(f'INSERT INTO lake.events SELECT * FROM read_parquet([{paths}])')
            con.execute('INSERT INTO lake.receipts VALUES (?,?)', [offset // 4, 4 * 65536])
            con.execute('COMMIT')
            batches.append(time.monotonic() - started)
            report['layouts'][name] = {'batch_seconds': batches, 'status': 'LOADING'}
            save(report)
        report['layouts'][name].update(verify(con, expected, count))
        assert con.execute('SELECT count(*),sum(rows) FROM lake.receipts').fetchone() == (4, count)
        report['layouts'][name]['files'] = con.execute(
            "SELECT count(*),sum(data_file_size_bytes) FROM ducklake_list_files('lake','events')").fetchall()
        report['layouts'][name]['extensions'] = con.execute(
            "SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded").fetchall()
        report['layouts'][name]['status'] = 'PASS'
        con.close()
        save(report)
    assert report['layouts']['local']['sample_sha256'] == report['layouts']['s3']['sample_sha256']
    # Deliberate multipart transfer, byte-range read and HEAD against a real server.
    payload = bytes(range(256)) * 32768
    upload = s3.create_multipart_upload(Bucket='qualification', Key='controls/multipart')['UploadId']
    parts = []
    for number, block in enumerate((payload[:5*2**20], payload[5*2**20:]), 1):
        response = s3.upload_part(Bucket='qualification', Key='controls/multipart', UploadId=upload,
                                  PartNumber=number, Body=block)
        parts.append({'PartNumber': number, 'ETag': response['ETag']})
    s3.complete_multipart_upload(Bucket='qualification', Key='controls/multipart', UploadId=upload,
                                MultipartUpload={'Parts': parts})
    assert s3.head_object(Bucket='qualification', Key='controls/multipart')['ContentLength'] == len(payload)
    response = s3.get_object(Bucket='qualification', Key='controls/multipart', Range='bytes=1048576-2097151')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 206
    assert response['Body'].read() == payload[2**20:2**21]
    objects = [o for page in s3.get_paginator('list_objects_v2').paginate(
        Bucket='qualification', Prefix='archive/') for o in page.get('Contents', [])]
    report['objects'] = [{'key': o['Key'], 'bytes': o['Size']} for o in objects]
    report['multipart_range_head_list'] = 'PASS'
    report['status'] = 'PASS_local_s3_equivalence_and_s3_primitives'
    save(report)
    print(json.dumps({'status': report['status'], 'rows': count,
                      'objects': len(objects), 'bytes': sum(o['Size'] for o in objects)}))


if __name__ == '__main__':
    main()
