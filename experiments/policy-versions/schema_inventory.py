"""Independent fresh Control inventory for the current-policy native case."""
import hashlib
import json
import re


CURRENT_SCHEMA = 41
CURRENT_MIGRATIONS = 57


def declared_inventory(source):
    manifest_path = source / 'migrations/manifest.json'
    manifest = json.loads(manifest_path.read_text())
    order = manifest['application_order']
    assert order[-1]['version'] == CURRENT_SCHEMA, 'current-policy schema profile changed'
    assert len(order) == CURRENT_MIGRATIONS, 'current-policy migration profile changed'
    tables = set()
    inputs = {'migrations/manifest.json': hashlib.sha256(manifest_path.read_bytes()).hexdigest()}
    for entry in order:
        name = 'migrations/postgres/' + entry['name']
        data = (source / name).read_bytes()
        inputs[name] = hashlib.sha256(data).hexdigest()
        tables.update(re.findall(r'^CREATE TABLE control\.([a-z_]+)\s*\(', data.decode(), re.MULTILINE))
    assert len(tables) == 82, 'current-policy Control table profile changed'
    return dict(target=CURRENT_SCHEMA, tables=sorted(tables),
                applied_versions=[entry['version'] for entry in order], inputs_sha256=inputs)


def require_tables(names, inventory):
    assert names == inventory['tables'], \
        f"Control inventory differs: missing={sorted(set(inventory['tables']) - set(names or []))}, " \
        f"unexpected={sorted(set(names or []) - set(inventory['tables']))}"
