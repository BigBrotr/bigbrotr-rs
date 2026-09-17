"""Nonsensitive lab inventory; do not serialize Docker environment/credentials."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

root = Path('/home/tux/bigbrotr-object-storage-20260917')
parser = argparse.ArgumentParser()
parser.add_argument('--label', required=True)
args = parser.parse_args()
assert args.label.replace('-', '').isalnum()
output = root / 'reports' / ('inventory-' + args.label + '.json')
assert not output.exists()
names = ['bigbrotr-object-storage-' + role + '-20260917' for role in ('garage', 'pg', 'worker', 'reader', 'ch-mv')]


def run(*command):
    return subprocess.run(command, text=True, capture_output=True, check=True).stdout


raw = json.loads(run('docker', 'inspect', *names))
containers = []
for item in raw:
    host = item['HostConfig']
    containers.append({'name': item['Name'], 'image_id': item['Image'],
                       'image_ref': item['Config']['Image'], 'state': item['State'],
                       'memory_limit': host['Memory'], 'memory_swap_limit': host['MemorySwap'],
                       'nano_cpus': host['NanoCpus'], 'ports': host['PortBindings'],
                       'network': host['NetworkMode'],
                       'mounts': [{k: m[k] for k in ('Type', 'Source', 'Destination', 'RW')}
                                  for m in item['Mounts']]})
stat = os.statvfs(root)
result = {'captured_at_unix': time.time(), 'containers': containers,
          'disk_available_bytes': stat.f_bavail * stat.f_frsize,
          'running_containers': run('docker', 'ps', '--format', '{{.Names}}').splitlines(),
          'memory': Path('/proc/meminfo').read_text(),
          'sources': [{'path': p.name, 'bytes': p.stat().st_size,
                       'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
                      for p in sorted((root / 'src').glob('*.py'))]}
worker = next(c for c in containers if 'worker-' in c['name'])
if worker['state']['Running']:
    result['worker_cgroup'] = run('docker', 'exec', worker['name'].lstrip('/'), 'python', '-c',
        "from pathlib import Path; import json; print(json.dumps({n:Path('/sys/fs/cgroup',n).read_text() "
        "for n in ('memory.current','memory.peak','memory.events','cpu.stat')}))")
    result['python_packages'] = run('docker', 'exec', worker['name'].lstrip('/'), '/lab/venv/bin/pip', 'freeze').splitlines()
    result['extension_hashes'] = run('docker', 'exec', worker['name'].lstrip('/'), 'python', '-c',
        "from pathlib import Path; import json,hashlib; print(json.dumps([{\"name\":p.name,"
        "\"bytes\":p.stat().st_size,\"sha256\":hashlib.sha256(p.read_bytes()).hexdigest()} "
        "for p in Path('/lab/extensions').rglob('*.duckdb_extension')]))")
output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'path': str(output), 'running_owned': sum(c['state']['Running'] for c in containers),
                  'disk_available_bytes': result['disk_available_bytes']}))
