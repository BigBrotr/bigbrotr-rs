"""S3 read failures, killed upload transaction and complete catalog/object restore."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

import psycopg2
from botocore.exceptions import ClientError
from probe import client, connect, verify

ROOT = Path('/lab')
credentials = json.loads((ROOT / 'credentials.json').read_text())


def aborted_child():
    con = connect('catalog_s3', 's3://qualification/archive/', credentials)
    con.execute('BEGIN')
    con.execute('INSERT INTO lake.events SELECT * FROM lake.events WHERE ordinal=0')
    con.execute('INSERT INTO lake.receipts VALUES (999,1)')
    print('READY_BEFORE_COMMIT', flush=True)
    sys.stdin.readline()


def digest_object(s3, key):
    body = s3.get_object(Bucket='qualification', Key=key)['Body']
    digest = hashlib.sha256()
    try:
        for block in body.iter_chunks(chunk_size=2**20):
            digest.update(block)
    finally:
        body.close()
    return digest.hexdigest()


def main():
    path = ROOT / 'reports/faults.json'
    assert not path.exists()
    report = {'status': 'RUNNING', 'cases': {}, 'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    def save():
        path.write_text(json.dumps(report, indent=2) + '\n')
    save()
    try:
        fixture = json.loads((ROOT / 'expected.json').read_text())
        s3 = client(credentials)
        bad = {**credentials, 'secret': '0' * 64}
        try:
            client(bad).get_object(Bucket='qualification', Key='controls/multipart')
        except ClientError as error:
            assert error.response['ResponseMetadata']['HTTPStatusCode'] == 403
            report['cases']['invalid_credentials'] = error.response['Error']['Code']
        else:
            raise AssertionError('Bad credentials were accepted')
        # Identify pre-existing catalog sessions before introducing one killed writer.
        pg = psycopg2.connect('dbname=qualification host=127.0.0.1 port=48732 user=postgres')
        pg.autocommit = True
        with pg.cursor() as cur:
            cur.execute("SELECT pid FROM pg_stat_activity WHERE datname='catalog_s3'")
            assert cur.fetchall() == [], 'Unexpected concurrent source catalog session'
        child = subprocess.Popen([sys.executable, __file__, '--child'], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        import selectors
        selector = selectors.DefaultSelector()
        selector.register(child.stdout, selectors.EVENT_READ)
        try:
            assert selector.select(120), 'Child did not reach pre-commit boundary'
            assert child.stdout.readline().strip() == 'READY_BEFORE_COMMIT'
            child.kill()
            child.communicate(timeout=20)
        finally:
            selector.close()
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=20)
        assert child.returncode == -9
        for _ in range(100):
            with pg.cursor() as cur:
                cur.execute("SELECT pid FROM pg_stat_activity WHERE datname='catalog_s3'")
                sessions = cur.fetchall()
            if not sessions:
                break
            time.sleep(0.1)
        assert not sessions, 'Old transaction authority still unresolved'
        con = connect('catalog_s3', 's3://qualification/archive/', credentials, readonly=True)
        assert con.execute('SELECT count(*) FROM lake.receipts WHERE batch=999').fetchone()[0] == 0
        assert con.execute('SELECT count(*) FROM lake.events').fetchone()[0] == fixture['rows']
        referenced = con.execute("SELECT data_file FROM ducklake_list_files('lake','events')").fetchall()
        con.close()
        report['cases']['killed_before_commit'] = {'signal': 9, 'old_pg_sessions': 0,
                                                   'visible_added_rows': 0, 'visible_receipts': 0}
        save()
        # Loss must be detected by a content read, not masked by metadata-only count(*).
        key = referenced[0][0].removeprefix('s3://qualification/')
        backup = 'controls/missing-file-backup'
        s3.copy_object(Bucket='qualification', Key=backup, CopySource={'Bucket': 'qualification', 'Key': key})
        s3.delete_object(Bucket='qualification', Key=key)
        try:
            con = connect('catalog_s3', 's3://qualification/archive/', credentials, readonly=True)
            try:
                con.execute('SELECT sum(octet_length(encode(content))) FROM lake.events').fetchone()
            except Exception as error:
                message = str(error)
                assert '404' in message or 'Not Found' in message or 'does not exist' in message, message[:200]
                report['cases']['missing_committed_object'] = {'refused': True, 'error_class': type(error).__name__}
            else:
                raise AssertionError('Missing committed body was silently accepted')
            finally:
                con.close()
        finally:
            s3.copy_object(Bucket='qualification', Key=key, CopySource={'Bucket': 'qualification', 'Key': backup})
        con = connect('catalog_s3', 's3://qualification/archive/', credentials, readonly=True)
        report['cases']['repaired_source'] = verify(con, fixture['events'], fixture['rows'])
        con.close()
        # Copy this quiescent catalog's committed closure; preserve relative paths.
        # Unreferenced files from aborted writes are explicitly excluded.
        con = connect('catalog_s3', 's3://qualification/archive/', credentials, readonly=True)
        all_files = con.execute("SELECT data_file FROM ducklake_list_files('lake','events') UNION ALL "
                                "SELECT data_file FROM ducklake_list_files('lake','receipts')").fetchall()
        con.close()
        copied = []
        started = time.monotonic()
        for (uri,) in all_files:
            source = uri.removeprefix('s3://qualification/')
            assert source.startswith('archive/')
            target = 'restored/' + source.removeprefix('archive/')
            s3.copy_object(Bucket='qualification', Key=target, CopySource={'Bucket': 'qualification', 'Key': source})
            left, right = digest_object(s3, source), digest_object(s3, target)
            assert left == right
            copied.append({'source': source, 'target': target, 'sha256': left,
                           'bytes': s3.head_object(Bucket='qualification', Key=target)['ContentLength']})
        con = connect('catalog_restore', 's3://qualification/restored/', credentials, readonly=True, override=True)
        restored_paths = con.execute("SELECT data_file FROM ducklake_list_files('lake','events')").fetchall()
        assert all(p[0].startswith('s3://qualification/restored/') for p in restored_paths)
        restored = verify(con, fixture['events'], fixture['rows'])
        assert con.execute('SELECT count(*),sum(rows) FROM lake.receipts').fetchone() == (4, fixture['rows'])
        con.close()
        report['cases']['restored_catalog_and_objects'] = {'copied': copied,
            'seconds': time.monotonic() - started, 'verification': restored,
            'scope': 'same-host alternate prefix and fresh catalog; not host-loss or live migration'}
        report['status'] = 'PASS_bounded_object_failure_and_restore_cases'
        save()
    except BaseException as error:
        report['status'] = 'FAILED'
        report['error'] = {'type': type(error).__name__, 'message': str(error)[:400]}
        save()
        raise
    print(json.dumps({'status': report['status'], 'cases': list(report['cases'])}))


if __name__ == '__main__':
    aborted_child() if '--child' in sys.argv else main()
