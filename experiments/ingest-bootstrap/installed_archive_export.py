"""Stopped initial owner -> ordinary history/export, with independent durable checks."""
import json
from pathlib import Path
import secrets
import shutil

import lab
from installed import messages, set_value, stop, terminal, wait_for
from installed_archive import catalog_rows, digest, files, global_roles, invoke


def run(pg, client, output, database, name, rows, setup, initialized, before):
    home = setup['home']
    destination = home / 'destination'
    destination.mkdir(mode=0o700)
    destination_id = secrets.token_hex(16)
    path = output / Path(setup['config']).name
    text = path.read_text()
    for section, key, value in [('archive.reader', 'enabled', True),
        ('export.destination', 'id', destination_id),
        ('export.destination', 'root', '/lab/' + name + '/destination')]:
        text = set_value(text, section, key, value)
    path.write_text(text)
    invoke(client, output, name + '-export-validate', 'config', 'validate', '--file', setup['config'])
    bound = invoke(client, output, name + '-destination', 'export', 'destination-initialize', '--file', setup['config'])
    assert bound['binding_sha256'] == digest(destination / 'destination.json')
    assert rows(database) == before
    catalog = catalog_rows(pg, setup['resource']['catalog_database'])
    roles = global_roles(pg)
    archives = {n: files(home / n) for n in ('data', 'state')}
    # Initialization/publication retain their own empty preparation directories.
    # Ordinary read/export must leave this original state unchanged.
    scratch = files(home / 'scratch')
    lab.write_json(output / (name + '-ordinary-scratch-before.json'), scratch)
    shown = invoke(client, output, name + '-export-settings', 'config', 'show', '--file', setup['config'])
    reserve = int(shown['settings']['runtime.disk_completion_reserve_bytes']['value'])
    assert reserve == 64*1024**3
    growth = 577*1024**2
    capacity = dict(reserve=reserve, growth=growth, free=shutil.disk_usage(home).free)
    lab.write_json(output / (name + '-export-capacity.json'), capacity)
    assert capacity['free'] >= reserve + growth
    lab.write_json(home / 'export-input.json', dict(scope=initialized['private_scope'],
        catalog_id=setup['resource']['catalog_id'], destination_id=destination_id))
    shutil.copy2(Path(__file__).with_name('installed_archive_export_client.py'),
        output / 'installed_archive_export_client.py')
    prefix = output / (name + '-ordinary')
    lab.docker('exec', '-d', client, 'python3', '/lab/installed_client.py', setup['config'],
        '/lab/' + prefix.name, 'control')
    wait_for(lambda: Path(str(prefix) + '.pid.json').exists(), 'ordinary Control start')
    first = wait_for(lambda: messages(prefix) or Path(str(prefix) + '.terminal.json').exists(), 'ordinary listener')
    assert isinstance(first, list) and first[0]['state'] == 'idle', first
    started = rows(database)
    lab.write_json(output / (name + '-ordinary-initialized.json'), started)
    try:
        result = lab.docker('exec', client, 'python3', '-B', '/lab/installed_archive_export_client.py',
            '/lab/' + name, check=False, timeout=40)
        (output / (name + '-export-client.stdout')).write_text(result.stdout)
        (output / (name + '-export-client.stderr')).write_text(result.stderr)
        # Capture observable durable effects even when a client assertion fails.
        observed = rows(database)
        lab.write_json(output / (name + '-export-observed.json'), observed)
        assert result.returncode == 0 and not result.stderr, 'ordinary signed export client failed'
    finally:
        if not Path(str(prefix) + '.terminal.json').exists():
            stop(client, prefix)
        ended = terminal(prefix)
        lab.write_json(output / (name + '-ordinary-after.json'), rows(database))
    assert ended['exit_code'] == 0 and not Path(str(prefix) + '.stderr').read_text()
    after = rows(database)
    final_catalog = catalog_rows(pg, setup['resource']['catalog_database'])
    final_roles = global_roles(pg)
    final_files = {n: files(home / n) for n in ('data', 'state')}
    final_scratch = files(home / 'scratch')
    lab.write_json(output / (name + '-ordinary-catalog.json'), final_catalog)
    lab.write_json(output / (name + '-ordinary-roles.json'), final_roles)
    lab.write_json(output / (name + '-ordinary-files.json'), final_files)
    lab.write_json(output / (name + '-ordinary-scratch-after.json'), final_scratch)
    from installed_archive_export_oracle import validate
    product = validate(before, after, home, initialized, setup['resource']['catalog_id'])
    assert final_catalog == catalog
    assert final_roles == roles
    assert final_files == archives
    assert final_scratch == scratch
    for db in (database, setup['resource']['catalog_database']):
        assert lab.sql(pg, db, 'SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()') == '0'
    lab.write_json(output / (name + '-export-verified.json'), product)
    return dict(status='PASS', terminal=ended, **product)
