"""Explicit0039 preparation and capture for the existing initial/live fixtures.

No new resource owner: bindings.py owns containers, and each existing capture
owns Serve and its joins. SQL cuts never reopen the living native store.
"""
import hashlib
import json
from pathlib import Path
import shutil
import sys
import time

SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/ingest-succession'))
import installed_progress_replay as typed
from installed import wait_for
from initial_history import inputs_at, server_clock
import lab


def record_source(output, build, revision):
    files = lab.command(['git','-C',str(SOURCE),'ls-files']).stdout.splitlines()
    assert files and len(files)==len(set(files))
    lab.write_json(output/'progress-source.json',dict(source=str(SOURCE),revision=revision,
        tree=lab.command(['git','-C',str(SOURCE),'rev-parse','HEAD^{tree}']).stdout.strip(),
        build=str(build),files_sha256={name:hashlib.sha256((SOURCE/name).read_bytes()).hexdigest()
            for name in files}))


def qualified_build(build, built):
    profile = typed.require_frozen_profile()
    assert built['source'] == profile['product_source']
    component = json.loads((build / 'component-result.json').read_text())
    qualification = json.loads((build.parent / 'qualification.json').read_text())
    assert qualification['status'] == 'PASS_PRODUCER_PROGRESS_COMPONENTS'
    assert component['status'] == 'PASS'
    for field in ('source', 'tree', 'source_manifest_sha256', 'lockfile_sha256'):
        assert component[field] == qualification[field] == built[field]
    binaries = component['executables_sha256']
    assert set(binaries) == {'bigbrotr', 'ingest_bootstrap_probe', 'native_components',
        'sync_progress', 'fact_codec', 'evidence_intent', 'control_schema', 'migrate_entrypoint'}
    assert all(hashlib.sha256((build / name).read_bytes()).hexdigest() == sha
        for name, sha in binaries.items())
    assert binaries['bigbrotr'] == built['binary_sha256']
    manifest_path = build.parent / 'build/source-manifest.json'
    assert hashlib.sha256(manifest_path.read_bytes()).hexdigest() == built['source_manifest_sha256']
    manifest = json.loads(manifest_path.read_text())
    assert manifest['revision'] == built['source'] and manifest['tree'] == built['tree']
    assert all(hashlib.sha256((SOURCE / name).read_bytes()).hexdigest() == sha
        for name, sha in manifest['files_sha256'].items()
        if name.startswith(('apps/', 'crates/', 'migrations/'))
        or name in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'))
    assert hashlib.sha256((SOURCE / 'migrations/postgres/0039_producer_progress.sql').read_bytes()
        ).hexdigest() == profile['migration_sha256']
    return dict(built, probe_name='ingest_bootstrap_probe',
        probe_sha256=binaries['ingest_bootstrap_probe'])


def catalog(pg, database):
    from history import catalog as complete_catalog
    return complete_catalog(pg, database)


def activation_acl(pg, database):
    # The invoker guard installed by0032 reads this table even for ordinary
    # epoch1 dataset/Control updates. Observe effective rights, not just ACL text.
    privileges = ('SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER')
    fields = []
    for privilege in privileges:
        for suffix in ('', ' WITH GRANT OPTION'):
            name = privilege + suffix
            fields.append("'" + name + "',has_table_privilege('bigbrotr_control',"
                "'control.initial_ingest_activations','" + name + "')")
    return lab.sql(pg, database, 'SELECT json_build_object(' + ','.join(fields) + ');')


def upgrade(pg, client, output, database, config, label, rows, *, reference_catalog=None):
    from initial_succession import physical
    from installed_progress_context_oracle import check_upgrade
    before, files = rows(database), physical(pg, database)
    context = dict(before=before, physical_before=files, status='INCOMPLETE',
        catalog_before=catalog(pg, database), responses=[])
    if reference_catalog is not None:
        context['reference_catalog32'] = reference_catalog
    try:
        for action in ('apply', 'reapply'):
            response = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
                '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator',
                '--target', '39')
            context['responses'].append(dict(action=action, returncode=response.returncode,
                stdout=response.stdout, stderr=response.stderr))
            assert response.returncode == 0 and not response.stderr
            state = rows(database)
            if action == 'reapply':
                assert state == context['after'] and physical(pg, database) == context['physical_after']
            else:
                context.update(after=state, physical_after=physical(pg, database))
        context['acl'] = dict(role='bigbrotr_control', relation='control.initial_ingest_activations',
            before_stdout=activation_acl(pg, database), grants=[])
        # Reuse the exact narrow read grant from the installed family owner.
        # Epoch2 already has SELECT/INSERT from initial_live.upgrade; no broader
        # activation/succession authority is needed by the initial entrypoint.
        for statement in ('GRANT SELECT ON control.initial_ingest_activations TO bigbrotr_control;',
                typed.PROGRESS_GRANTS):
            context['acl']['grants'].append(dict(statement=statement,
                stdout=lab.sql(pg, database, statement)))
        context['acl']['after_stdout'] = activation_acl(pg, database)
        assert rows(database) == context['after'] and physical(pg, database) == context['physical_after']
        context['catalog_after'] = catalog(pg, database)
        check_upgrade(context)
        context['status'] = 'APPLIED_EXACT39'
    finally:
        # Exceptions retain incomplete/unknown setup; there is no retry or adoption.
        if 'acl' in context:
            lab.write_json(output / (label + '-progress-acl.json'), context['acl'])
        lab.write_json(output / (label + '-progress-upgrade.json'), context)
    return context


def helpers(output, directory, kind):
    assert kind in ('initial', 'activated')
    for source in (Path(__file__).with_name('installed_progress_transport.py'),
            SOURCE / 'experiments/ingest-succession/installed_family_client.py'):
        target = output / source.name
        if not target.exists():
            shutil.copy2(source, target)
        assert target.read_bytes() == source.read_bytes()
    lab.write_json(directory / 'progress-context.json', dict(format='bigbrotr-progress-context/1', kind=kind))


def phases(pg, database, directory, rows, snapshot, kind, prefix):
    from installed_family import capture_progress_observation, release_progress_transport
    from installed_progress_context_oracle import ready
    for phase in ('mutation', 'proof-only'):
        def observed():
            assert not (directory / 'relay-error.json').exists(), 'split peer failed'
            assert not Path(str(prefix) + '.terminal.json').exists(), 'Serve ended before typed phase'
            wires = [json.loads(path.read_text()) for path in sorted(directory.glob('relay-closed-*.json'))]
            matches = [wire for wire in wires if wire.get('progress_phase') == phase]
            assert len(matches) <= 1, 'replayed typed phase'
            return matches[0] if matches and ready(rows(database), kind, phase, matches[0]) else None
        try:
            wire = wait_for(observed, 'typed ' + kind + ' ' + phase, 10)
        except BaseException as error:
            failure = dict(status='UNQUALIFIED_PHASE_FAILURE',phase=phase,error=type(error).__name__,
                native='NOT_REOPENED',custody='EXISTING_OUTER_OWNER_REQUIRED')
            try:
                failure['control'] = rows(database)
                failure['inputs'] = inputs_at(directory / 'store')
            except BaseException as observation_error:
                failure['observation_error'] = type(observation_error).__name__
            lab.write_json(directory / ('progress-'+phase+'-failure.json'),failure)
            raise
        cut = snapshot('progress-' + phase)
        assert ready(cut['control'], kind, phase, wire)
        captured = capture_progress_observation(directory, phase, cut, wire)
        from initial_succession import census
        lab.write_json(directory / ('progress-'+phase+'-input-files.json'), census(directory / 'store/producer-inputs-v1'))
        if phase == 'mutation':
            release_progress_transport(directory, captured)


class InitialCapture:
    def __init__(self, pg, database, output, name, rows, setup):
        self.pg, self.database, self.output, self.name = pg, database, output, name
        self.directory, self.raw_rows, self.setup = output / name, rows, setup
        self.snapshots = []
        self.before = rows(database)

    def rows(self, database):
        assert database == self.database
        return typed.legacy_state(self.raw_rows(database))

    def snapshot(self, stage):
        item = dict(stage=stage, server_us=server_clock(self.pg, self.database),
            control=self.raw_rows(self.database), inputs=inputs_at(self.directory / 'store'))
        item['observed_by_us'] = server_clock(self.pg, self.database)
        self.snapshots.append(item)
        lab.write_json(self.output / (self.name+'-progress-'+stage+'-sql.json'),item['control'])
        lab.write_json(self.output / (self.name + '-progress-observations.json'), self.snapshots)
        return item

    def exclude(self, after):
        from initial_history import observe_backend_absence
        from installed_empty import exact_absence
        absence = observe_backend_absence(self.pg,self.database,self.output,self.name)
        expected = [dict(pid=row['control_backend_pid'],backend_start=row['control_backend_start'],
            database=self.database,control_epoch=row['epoch'])
            for row in after['owner_epochs'] if row['role']=='control']
        assert len(expected)==1
        self.backend = dict(absence=absence,exact=exact_absence(self.pg,self.database,expected),
            observed_monotonic_ns=time.monotonic_ns())
        lab.write_json(self.output / (self.name+'-backend-exclusion.json'),self.backend)

    def finish(self, initialized, transcript, ended, signal, native_result):
        from installed_progress_context_oracle import verify_initial, controls
        from initial_succession import census
        prefix = self.output / self.name
        after = self.raw_rows(self.database)
        lab.write_json(self.output / (self.name+'-progress-after-sql.json'),after)
        context = dict(kind='initial', setup=self.setup, before=self.before, after=after,
            snapshots=self.snapshots, report=initialized, transcript=transcript,
            opened=[json.loads(p.read_text()) for p in sorted(self.directory.glob('relay-open-*.json'))],
            terminal=ended, stdout=Path(str(prefix) + '.stdout').read_text(),
            stderr=Path(str(prefix) + '.stderr').read_text(), signal=signal,
            client_terminal=json.loads((self.directory / 'client-exit.json').read_text()),
            client_stdout=(self.directory / 'client.stdout').read_text(),
            client_stderr=(self.directory / 'client.stderr').read_text(),
            observations=json.loads((self.directory / 'submitted.json').read_text()),
            inputs=inputs_at(self.directory / 'store'), input_contents={p.name:p.read_bytes().hex()
                for p in (self.directory / 'store/producer-inputs-v1').iterdir()},
            native=json.loads((self.output / (self.name + '-native.json')).read_text()),
            native_readback=json.loads((self.output / (self.name+'-dump-response.json')).read_text()),
            event_bytes=(self.directory / 'event.jsonl').read_bytes().hex(),
            input_files={phase:json.loads((self.directory / ('progress-'+phase+'-input-files.json')).read_text())
                for phase in ('mutation','proof-only')},
            input_files_after=census(self.directory / 'store/producer-inputs-v1'),
            native_result=native_result, backend=self.backend, catalog_after=catalog(self.pg, self.database),
            backend_count=int(lab.sql(self.pg, self.database, "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname=current_database() AND usename='bigbrotr_control'")),
            gates={name:(self.directory / name).read_text() for name in (
                'progress-mutation-observation.json', 'progress-proof-only-observation.json', 'progress-release.json')})
        path = self.output / (self.name + '-progress-verification.json')
        lab.write_json(self.output/(self.name+'-progress-catalog.json'),context['catalog_after'])
        lab.write_json(path, context)  # Raw context survives verifier failure.
        answer = verify_initial(context)
        negative = controls(context, verify_initial)
        return dict(oracle=answer, negative_controls=negative,
            verification_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
