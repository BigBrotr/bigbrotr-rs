"""Run in another container with no source/local-archive/Garage data mounts."""
import json
from pathlib import Path

from botocore.exceptions import ClientError
from probe import client, connect, verify

credentials = json.loads(Path('/run/reader.json').read_text())
fixture = json.loads(Path('/app/expected.json').read_text())
con = connect('catalog_s3', 's3://qualification/archive/', credentials, readonly=True,
              user='archive_reader', extensions='/extensions')
result = verify(con, fixture['events'], fixture['rows'])
result['receipts'] = con.execute('SELECT count(*),sum(rows) FROM lake.receipts').fetchone()
assert result['receipts'] == (4, fixture['rows'])
try:
    client(credentials).put_object(Bucket='qualification', Key='controls/forbidden', Body=b'forbidden')
except ClientError as error:
    result['write_refusal'] = error.response['Error']['Code']
    assert error.response['ResponseMetadata']['HTTPStatusCode'] == 403
else:
    raise AssertionError('Read-only object credentials allowed a write')
result['status'] = 'PASS_separate_reader_without_local_data_mount'
Path('/reports/reader.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
