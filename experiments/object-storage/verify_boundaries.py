"""Additional lab checks; reuse existing catalogs, never reload scientific data.

Run with the existing lab venv: python verify_boundaries.py --run-id UNIQUE_LABEL
Keep publishers/maintenance stopped. Objects created under controls/boundaries-LABEL/
are retained as run evidence. This is not a live backup or durability qualification.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time

ROOT = Path('/lab')
BUCKET = 'qualification'
MIB = 2**20


class CheckFailed(Exception):
    pass


def require(condition, message):
    if not condition:
        raise CheckFailed(message)


def write_report(path, report, fresh=False):
    """Publish complete JSON atomically; initial publication cannot replace evidence."""
    pending = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', dir=path.parent,
                                         prefix=path.name + '.', delete=False) as output:
            pending = Path(output.name)
            json.dump(report, output, indent=2)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        if fresh:
            os.link(pending, path)
        else:
            os.replace(pending, path)
    finally:
        if pending is not None:
            pending.unlink(missing_ok=True)


def payload_bytes():
    # Position enters every block's preimage; aligned wrong-offset reads differ.
    size = 8 * MIB + 137
    return b''.join(hashlib.shake_256(b'object-storage-boundaries-v1:' +
                                    index.to_bytes(8, 'big')).digest(4096)
                    for index in range((size + 4095) // 4096))[:size]


def verify_body(response, expected, status, content_range=None):
    try:
        require(response['ResponseMetadata']['HTTPStatusCode'] == status, 'Unexpected HTTP status')
        require(response['ContentLength'] == len(expected), 'Response length differs')
        if content_range is not None:
            require(response.get('ContentRange') == content_range, 'Content-Range differs')
        actual = response['Body'].read(len(expected) + 1)
        require(actual == expected, 'Response bytes differ (offset, truncation or corruption)')
        return hashlib.sha256(actual).hexdigest()
    finally:
        response['Body'].close()


def primitives(s3, key):
    from botocore.exceptions import ClientError
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError as error:
        require(error.response['ResponseMetadata']['HTTPStatusCode'] == 404,
                'Cannot establish that the control object is new')
    else:
        raise CheckFailed('Refusing to overwrite an existing control object')
    payload = payload_bytes()
    upload = s3.create_multipart_upload(Bucket=BUCKET, Key=key)['UploadId']
    complete = False
    try:
        parts = []
        for number, block in enumerate((payload[:5*MIB], payload[5*MIB:]), 1):
            response = s3.upload_part(Bucket=BUCKET, Key=key, UploadId=upload,
                                      PartNumber=number, Body=block)
            # ETags are opaque multipart tokens, not assumed content digests.
            parts.append({'PartNumber': number, 'ETag': response['ETag']})
        s3.complete_multipart_upload(Bucket=BUCKET, Key=key, UploadId=upload,
                                    MultipartUpload={'Parts': parts})
        complete = True
    finally:
        if not complete:
            s3.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=upload)
    require(s3.head_object(Bucket=BUCKET, Key=key)['ContentLength'] == len(payload), 'HEAD size differs')
    digest = verify_body(s3.get_object(Bucket=BUCKET, Key=key), payload, 200)
    ranges = [(5*MIB-31, 5*MIB+47), (len(payload)-257, len(payload)-1)]
    for start, end in ranges:
        verify_body(s3.get_object(Bucket=BUCKET, Key=key, Range=f'bytes={start}-{end}'),
                    payload[start:end+1], 206, f'bytes {start}-{end}/{len(payload)}')
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key, Range=f'bytes={len(payload)}-')
    except ClientError as error:
        require(error.response['ResponseMetadata']['HTTPStatusCode'] == 416,
                'Unsatisfiable range did not return HTTP 416')
        invalid_code = error.response['Error']['Code']
    else:
        response['Body'].close()
        raise CheckFailed('Unsatisfiable range was accepted')
    return {'key': key, 'bytes': len(payload), 'sha256': digest,
            'ranges': ranges, 'invalid_range_code': invalid_code}


def readonly_objects(s3, reader, key):
    from botocore.exceptions import ClientError
    payload = payload_bytes()
    start, end = len(payload)-257, len(payload)-1
    verify_body(reader.get_object(Bucket=BUCKET, Key=key, Range=f'bytes={start}-{end}'),
                payload[start:], 206, f'bytes {start}-{end}/{len(payload)}')
    refused = {}
    for name, operation in (
        ('put', lambda: reader.put_object(Bucket=BUCKET, Key=key + '-denied-put', Body=b'forbidden')),
        ('delete', lambda: reader.delete_object(Bucket=BUCKET, Key=key)),
        ('create_multipart', lambda: reader.create_multipart_upload(Bucket=BUCKET, Key=key + '-denied-multipart')),
    ):
        try:
            response = operation()
        except ClientError as error:
            require(error.response['ResponseMetadata']['HTTPStatusCode'] == 403,
                    'Read-only operation failed without authorization refusal: ' + name)
            refused[name] = error.response['Error']['Code']
        else:
            if name == 'create_multipart':
                s3.abort_multipart_upload(Bucket=BUCKET, Key=key + '-denied-multipart',
                                         UploadId=response['UploadId'])
            raise CheckFailed('Read-only credentials allowed ' + name)
    verify_body(s3.get_object(Bucket=BUCKET, Key=key), payload, 200)
    return {'successful_read': True, 'refusals': refused}


def readonly_catalog(run_id):
    import psycopg2
    from psycopg2 import sql
    con = psycopg2.connect(dbname='catalog_s3', host='127.0.0.1', port=48732,
                           user='archive_reader', connect_timeout=10)
    refused = {}
    try:
        with con.cursor() as cur:
            cur.execute('SET default_transaction_read_only = off')
            cur.execute("SET statement_timeout = '10s'")
            con.commit()
            for name, statement in (
                ('insert', 'INSERT INTO public.ducklake_metadata SELECT * FROM public.ducklake_metadata WHERE false'),
                ('update', 'UPDATE public.ducklake_metadata SET value=value WHERE false'),
                ('delete', 'DELETE FROM public.ducklake_metadata WHERE false'),
                ('create_table', sql.SQL('CREATE TABLE public.{} (value INTEGER)').format(
                    sql.Identifier('boundary_' + run_id))),
            ):
                cur.execute('SET TRANSACTION READ WRITE')
                cur.execute('SHOW transaction_read_only')
                require(cur.fetchone()[0] == 'off', 'PG denial would only test read-only transaction mode')
                cur.execute('SELECT current_user, count(*) FROM public.ducklake_metadata')
                require(cur.fetchone()[0] == 'archive_reader', 'Unexpected PG role')
                try:
                    cur.execute(statement)
                except psycopg2.Error as error:
                    require(error.pgcode == '42501', 'PG write failed without insufficient_privilege: ' + name)
                    refused[name] = error.pgcode
                else:
                    raise CheckFailed('PG reader role allowed ' + name)
                finally:
                    # Even unexpected success cannot commit catalog data or DDL.
                    con.rollback()
    finally:
        con.close()
    return {'refusals': refused, 'transaction_read_only': 'off',
            'scope': 'role grants only; loopback trust authentication is not authenticated isolation'}


def catalog_closure(database, prefix, credentials):
    from probe import connect
    con = connect(database, f's3://{BUCKET}/{prefix}', credentials, readonly=True,
                  override=database == 'catalog_restore')
    files = {}
    try:
        con.execute('BEGIN')
        snapshot = con.execute("SELECT id FROM ducklake_current_snapshot('lake')").fetchone()[0]
        for family in ('events', 'receipts'):
            rows = con.execute("SELECT data_file,data_file_size_bytes,delete_file,delete_file_size_bytes "
                               "FROM ducklake_list_files('lake', ?)", [family]).fetchall()
            require(rows, 'Missing expected catalog family: ' + family)
            for row in rows:
                for kind, uri, size in (('data', row[0], row[1]), ('delete', row[2], row[3])):
                    if uri is None:
                        continue
                    root = f's3://{BUCKET}/{prefix}'
                    require(uri.startswith(root), 'Catalog file is outside its expected root')
                    identity = (family, kind, uri[len(root):])
                    require(identity not in files or files[identity] == size, 'Conflicting file sizes')
                    files[identity] = size
        receipts = con.execute('SELECT batch,rows FROM lake.receipts ORDER BY batch').fetchall()
        con.execute('COMMIT')
        require(con.execute("SELECT id FROM ducklake_current_snapshot('lake')").fetchone()[0] == snapshot,
                'Catalog changed during closure inspection')
    finally:
        con.close()
    return {'snapshot': snapshot, 'receipts': receipts, 'files': [
        {'family': family, 'kind': kind, 'relative_path': path, 'bytes': size}
        for (family, kind, path), size in sorted(files.items())]}


def closure_and_listing(s3, credentials):
    source = catalog_closure('catalog_s3', 'archive/', credentials)
    restored = catalog_closure('catalog_restore', 'restored/', credentials)
    require(source == restored, 'Source/restored snapshot, receipts or complete family closure differs')
    expected_rows = json.loads((ROOT / 'expected.json').read_text())['rows']
    require(source['receipts'] == [(index, expected_rows // 4) for index in range(4)],
            'Unexpected retained batch receipts')
    listings = {}
    for prefix in ('archive/', 'restored/'):
        expected = {prefix + item['relative_path']: item['bytes'] for item in source['files']}
        listed = {}
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=prefix):
            for item in page.get('Contents', []):
                require(item['Key'] not in listed, 'Duplicate LIST entry')
                listed[item['Key']] = item['Size']
        require(expected and all(listed.get(key) == size for key, size in expected.items()),
                'LIST omitted or mis-sized a catalog-selected object')
        for key, size in expected.items():
            require(s3.head_object(Bucket=BUCKET, Key=key)['ContentLength'] == size,
                    'Catalog/LIST/HEAD sizes differ')
        listings[prefix] = {'expected': expected, 'listed_count': len(listed),
                            'extra_keys': sorted(set(listed) - set(expected))}
    return {'source_and_restored': source, 'listings': listings,
            'scope': 'same-host two-family path/metadata closure; source remains accessible; no live backup barrier'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}', args.run_id), 'Invalid run identity')
    path = ROOT / 'reports' / f'boundaries-{args.run_id}.json'
    report = {'status': 'RUNNING', 'run_id': args.run_id, 'started_at_unix': time.time(),
              'sources': {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                          for name in ('verify_boundaries.py', 'probe.py')},
              'cases': {}, 'stage': 'preconditions',
              'limits': ['No load rerun or scientific-data mutation', 'No interrupted-upload/ambiguous-COMMIT test',
                         'No independent-host, source-denied, power-loss or durability-class qualification']}
    write_report(path, report, fresh=True)
    try:
        from probe import client
        report['inputs'] = {}
        for name in ('object-storage.json', 'catalog-copy.json', 'faults.json'):
            data = (ROOT / 'reports' / name).read_bytes()
            require(json.loads(data)['status'].startswith('PASS_'), 'Required prior run is not PASS: ' + name)
            report['inputs'][name] = hashlib.sha256(data).hexdigest()
        credentials = json.loads((ROOT / 'credentials.json').read_text())
        reader_credentials = json.loads((ROOT / 'reader.json').read_text())
        s3, reader = client(credentials), client(reader_credentials)
        key = f'controls/boundaries-{args.run_id}/payload'
        for stage, action in (
            ('multipart_ranges', lambda: primitives(s3, key)),
            ('readonly_objects', lambda: readonly_objects(s3, reader, key)),
            ('readonly_catalog', lambda: readonly_catalog(args.run_id)),
            ('catalog_closure_and_listing', lambda: closure_and_listing(s3, credentials)),
        ):
            report['stage'] = stage
            write_report(path, report)
            report['cases'][stage] = {'status': 'PASS', **action()}
            write_report(path, report)
        report['status'] = 'PASS_additional_object_boundaries'
        report['stage'] = 'complete'
    except BaseException as error:
        report['status'] = 'FAILED'
        # Engine exceptions can embed credentials/SQL: retain class and controlled messages only.
        report['error'] = {'type': type(error).__name__}
        if isinstance(error, CheckFailed):
            report['error']['message'] = str(error)
        report['finished_at_unix'] = time.time()
        write_report(path, report)
        print(json.dumps({'status': report['status'], 'stage': report['stage'], 'report': str(path)}))
        return 1
    report['finished_at_unix'] = time.time()
    write_report(path, report)
    print(json.dumps({'status': report['status'], 'report': str(path)}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
