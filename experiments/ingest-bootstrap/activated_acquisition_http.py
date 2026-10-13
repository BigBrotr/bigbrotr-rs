"""One genuine authenticated HTTP/Sync journey under schema36 without gap grants.

Activate with the established original grants, then remove recovery privileges
before login/acceptance/claim. Qualified restoration follows actual process and
backend exclusion; failed custody is retained. No raw historical claim supplies
HTTP evidence.
"""
import json
from pathlib import Path
import shutil
import subprocess

import lab
import activated_acquisition as acquisition
import activated_recovery as recovery
import initial_succession as preparing
import installed_live as live
from activated_acquisition_http_oracle import MODE, REVOKE, RESTORE, verify, negative_controls
from initial_history import inputs_at, observe_backend_absence, server_clock


def upgrade(pg, client, output, database, config, rows):
    value = dict(before=rows(database), physical_before=preparing.physical(pg, database),
        catalog_before=live.catalog(pg, database), responses=[], server_window={})
    value['server_window']['before_us'] = server_clock(pg, database)
    try:
        for stage in ('after', 'reapplied'):
            answer = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
                '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '36')
            value['responses'].append(dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr))
            value[stage] = rows(database)
            lab.write_json(output / (MODE + '-migration.json'), value)
            assert answer.returncode == 0 and not answer.stderr
        value.update(physical_after=preparing.physical(pg, database), catalog_after=acquisition.catalog(pg, database))
        value['server_window']['after_us'] = server_clock(pg, database)
        return value
    except subprocess.TimeoutExpired as error:
        def text(raw):
            return raw.decode(errors='replace') if isinstance(raw, bytes) else raw or ''
        value.update(outcome='UNKNOWN_TIMEOUT', incomplete_response=dict(returncode=None,
            stdout=text(error.stdout), stderr=text(error.stderr)))
        raise
    finally:
        lab.write_json(output / (MODE + '-migration.json'), value)


def run_all(pg, client, output, rows, clone):
    basis = output.parent.parent / preparing.SEED / 'bindings'
    store = basis / 'retire_signed/store'
    assert all(preparing.digest(basis / n) == h for n, h in preparing.FROZEN.items())
    source = json.loads((basis / 'result.json').read_text())
    assert source['status'] == 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE'
    assert source['source'] == preparing.SOURCE and source['owner'] == preparing.OWNER
    assert not source['cleanup_errors'] and not source['unrelated_changed']
    containers = json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)
    old, = [c for c in containers if c['Id'] == source['client']['id']]
    assert old['Config']['Labels'][source['label']] == preparing.OWNER
    assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
    assert not any(c['State']['Running'] and any(str(basis) in m.get('Source', '') for m in c['Mounts']) for c in containers)
    absent = observe_backend_absence(pg, preparing.DATABASE, output, MODE + '-source')
    original = rows(preparing.DATABASE)
    assert original == json.loads((basis / 'retire-signed-retry-after.json').read_text())
    seed = dict(control=original, native=json.loads((basis / 'retire-signed-retry-native.json').read_text()),
        inputs=inputs_at(store), sequence=58)
    origin = dict(files=preparing.census(store), contents=recovery.contents_at(store), client_state=old['State'],
        control=original, inputs=seed['inputs'], frozen=preparing.FROZEN, backend_absence=absent)
    lab.write_json(output / (MODE + '-source.json'), origin)
    root = Path(__file__).resolve().parents[2]
    for name in live.HELPERS:
        target = output / name
        if not target.exists():
            shutil.copy2(Path(__file__).with_name(name), target)
        assert preparing.digest(target) == preparing.digest(Path(__file__).with_name(name))
    database = None
    acl = dict(revocation=None, final=None, restoration=None)
    raw = None
    try:
        database = clone(MODE, preparing.DATABASE)
        assert rows(database) == original
        directory = output / MODE
        directory.mkdir(mode=0o700)
        shutil.copytree(store, directory / 'store')
        shutil.copy2(basis / 'signed-seed/event.jsonl', directory / 'event.jsonl')
        shutil.copy2(root / 'tests/fixtures/acquisition/selector-events.json', directory / 'selector-events.json')
        observe_backend_absence(pg, database, output, MODE + '-before')
        config = live.configure(pg, client, output, database, directory, original)
        live.upgrade(pg, client, output, database, config, MODE, rows)
        policy = live.policy(pg, database, output, MODE, rows, False)
        migration = upgrade(pg, client, output, database, config, rows)

        def after_activated(snapshot):
            value = dict(statement=REVOKE, activated=snapshot,
                before=acquisition.catalog(pg, database), server_window={}, response=None, after=None)
            acl['revocation'] = value
            lab.write_json(output / (MODE + '-acl.json'), acl)
            value['server_window']['before_us'] = server_clock(pg, database)
            try:
                value['response'] = recovery.statement(pg, database, output, MODE + '-revoke', REVOKE)
                value['after'] = acquisition.catalog(pg, database)
                value['server_window']['after_us'] = server_clock(pg, database)
                assert value['response'] == dict(returncode=0, stdout='', stderr='', outcome='PROCESS_EXITED')
                privileges = value['after']['acquisition']['privileges']
                assert all(privileges[k] is False for k in ('gap_select', 'gap_insert', 'gap_update', 'gap_delete', 'gap_truncate'))
                assert all(not f['execute'] for f in value['after']['functions'] if f['name'] in (
                    'control.require_activated_initial_recovery(bytea)', 'control.require_activated_initial_source(bytea)'))
                assert all(not f['execute'] for f in value['after']['acquisition']['functions'] if f['name'] in (
                    'control.guard_acquisition_gap()', 'control.require_acquisition_gap_complete()'))
                return value
            finally:
                lab.write_json(output / (MODE + '-acl.json'), acl)

        raw = live.capture_case(pg, client, output, rows, MODE, database, config, directory,
            seed, policy, positive=True, after_activated=after_activated)
        acl['final'] = acquisition.catalog(pg, database)
        lab.write_json(output / (MODE + '-acl.json'), acl)
    finally:
        # The capture owner always joins or records unknown custody. Privilege
        # restoration is DCL only; it cannot authorize a native reopen.
        if acl['revocation'] is not None:
            restored = dict(statement=RESTORE, response=None, catalog=None, errors=[])
            acl['restoration'] = restored
            try:
                restored['response'] = recovery.statement(pg, database, output, MODE + '-restore', RESTORE)
                restored['catalog'] = acquisition.catalog(pg, database)
            except BaseException as error:
                restored['errors'].append(type(error).__name__)
            lab.write_json(output / (MODE + '-acl.json'), acl)
        old_now, = json.loads(lab.docker('inspect', old['Id']).stdout)
        preserved = dict(control=rows(preparing.DATABASE) == original,
            files=preparing.census(store) == origin['files'], inputs=inputs_at(store) == origin['inputs'],
            contents=recovery.contents_at(store) == origin['contents'], client=old_now['State'] == origin['client_state'],
            artifacts=all(preparing.digest(basis / n) == h for n, h in preparing.FROZEN.items()))
        lab.write_json(output / (MODE + '-source-preserved.json'), preserved)
        assert all(preserved.values()), preserved
    context = dict(capture=raw, migration=migration, acl=acl)
    lab.write_json(output / (MODE + '-http-verification.json'), context)
    checked, controls = verify(**context), negative_controls(**context)
    return dict(name='activated-acquisition-http', status='PASS', cli_invocations=1,
        cases=[dict(name=MODE, status='PASS', oracle=checked, negative_controls=controls)],
        source_preserved=preserved, helper_sha256={n:preparing.digest(Path(__file__).with_name(n)) for n in live.HELPERS},
        scope='authenticated HTTP acceptance and genuine current-owner claim under schema36 without recovery grants',
        not_run=['job completion or coverage', 'new historical claims', 'retirement', 'Archive restart and full M1'])
