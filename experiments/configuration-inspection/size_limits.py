#!/usr/bin/env python3
"""MON-005: exact JSONB bounds through the actual CLI and PostgreSQL 16.15."""
import argparse
import copy
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab

CAP = 10 * 1024 * 1024


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def compact(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def rendered(value):
    # Independent oracle: Python decimal expands numeric tokens; no Rust size
    # counter or PostgreSQL rendering is called to manufacture expected lengths.
    if isinstance(value, Decimal):
        return format(value, 'f')
    if isinstance(value, dict):
        return '{' + ', '.join(json.dumps(k, ensure_ascii=False) + ': ' + rendered(v)
                               for k, v in value.items()) + '}'
    if isinstance(value, list):
        return '[' + ', '.join(rendered(v) for v in value) + ']'
    return json.dumps(value, ensure_ascii=False)


def identity(value):
    raw = compact(value).encode()
    return raw, hashlib.sha256(b'bigbrotr/product-policy/1\0' + raw).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    root = parser.parse_args().root.resolve(strict=True)
    state = lab.load(root)
    require(Path(state['source']) == SOURCE, 'harness source path')
    require(not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout, 'dirty source')
    revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
    require(revision == state['build']['build_info']['source']['revision'], 'source revision')
    actual = lab.cli(state, 'build-info')
    require(actual.returncode == 0 and json.loads(actual.stdout) == state['build']['build_info'], 'artifact metadata')
    mounts = json.loads(lab.docker('inspect', state['client']).stdout)[0]['Mounts']
    build = Path(next(m['Source'] for m in mounts if m['Destination'] == '/build' and not m['RW']))
    require(hashlib.sha256((build / 'bigbrotr').read_bytes()).hexdigest() == state['binary_sha256'], 'artifact bytes')
    output = root / 'configuration-size.json'
    require(not output.exists(), 'existing report')
    report = dict(format='bigbrotr-configuration-size/1', status='RUNNING', source=revision,
                  binary_sha256=state['binary_sha256'], server=state['server'], cases=[])

    def save():
        lab.write_json(output, report)

    def case(name, **values):
        report['cases'].append(dict(name=name, status='PASS', **values)); save()

    def prepare(name):
        db = 'bb_size_' + name
        lab.sql(state, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator;')
        path = lab.config(root, state, db)
        local = root / Path(path).name
        local.write_text(lab.change(lab.change(local.read_text(), 'postgres.control', 'password_ref',
                                              'file:/lab/secrets/application'), 'administration', 'enabled', False))
        require(lab.migrate(state, 'apply', path, target=802).returncode == 0, 'migration')
        lab.sql(state, db, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; '
                'GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control; '
                'GRANT INSERT ON control.datasets, control.config_revisions, control.principals, '
                'control.management_audit, control.policy_revisions, control.management_commands TO bigbrotr_control; '
                'GRANT UPDATE(config_revision,policy_revision) ON control.datasets TO bigbrotr_control;')
        return db, path, local

    def execute(db, path, name, *args, readonly=False):
        before = lab.schema_digest(state, db) if readonly else None
        result = lab.cli(state, 'config', *args, '--file', path)
        (root / (name + '.stdout')).write_text(result.stdout)
        (root / (name + '.stderr')).write_text(result.stderr)
        if readonly:
            require(before == lab.schema_digest(state, db), name + ' changed database')
        return result

    def accepted(db, path, name, policy):
        raw, digest = identity(policy)
        require(len(raw) <= CAP, 'compact input cap')
        (root / (name + '.policy.json')).write_bytes(raw)
        validated = execute(db, path, name + '-validate', 'validate', readonly=True)
        require(validated.returncode == 0 and not validated.stderr, name + ' static refusal')
        candidate = json.loads(validated.stdout)['initial_policy']
        require(candidate['sha256'] == digest and int(candidate['encoded_bytes']) == len(raw), 'independent candidate identity')
        initialized = execute(db, path, name + '-initialize', 'initialize')
        require(initialized.returncode == 0 and json.loads(initialized.stdout)['status'] == 'initialized', name + ' initialize')
        require(json.loads(initialized.stdout)['initial_policy_sha256'] == digest, 'initialized identity')
        before = lab.schema_digest(state, db)
        retry = execute(db, path, name + '-retry', 'initialize', readonly=True)
        require(retry.returncode == 0 and json.loads(retry.stdout)['status'] == 'already-initialized', 'initialization retry')
        for phase in ['before', 'after']:
            inspected = execute(db, path, name + '-inspect-' + phase, 'inspect', readonly=True)
            require(inspected.returncode == 0 and not inspected.stderr, 'inspection refusal')
            observed = json.loads(inspected.stdout)
            require(observed['accepted_policy_sha256'] == digest and not observed['candidate_policy_differs']
                    and observed['desired_revision'] == '1'
                    and observed['security_revision'] == (None if phase == 'before' else '1'), 'inspection identity')
            if phase == 'before':
                activated = execute(db, path, name + '-activate', 'activate-initial', '--request-id', '75' * 16, '--expected-revision', '1')
                require(activated.returncode == 0 and json.loads(activated.stdout)['status'] == 'activated', 'initial activation')
        # Full stored documents, independently decoded and compared, not just logs.
        stored = json.loads(lab.sql(state, db, "SELECT json_build_object('config',(SELECT document FROM control.config_revisions),'policy',(SELECT document FROM control.policy_revisions),'digests',(SELECT json_agg(encode(document_digest,'hex')) FROM (SELECT document_digest FROM control.config_revisions UNION ALL SELECT document_digest FROM control.policy_revisions) d));"))
        require(stored['config'] == policy == stored['policy'] and stored['digests'] == [digest, digest], 'stored policy mismatch')
        rendered_bytes = int(lab.sql(state, db, 'SELECT octet_length(document::text) FROM control.config_revisions;'))
        expected = len(rendered(json.loads(raw, parse_float=Decimal)).encode())
        require(rendered_bytes == expected <= CAP, 'persisted size oracle')
        snapshot = lab.schema_digest(state, db)
        retry = execute(db, path, name + '-activate-retry', 'activate-initial', '--request-id', '75' * 16, '--expected-revision', '1', readonly=True)
        require(retry.returncode == 0 and json.loads(retry.stdout)['status'] == 'already-activated', 'activation retry')
        lab.write_json(root / (name + '.stored.json'), stored)
        case(name, compact_bytes=len(raw), jsonb_bytes=rendered_bytes, policy_sha256=digest,
             initialized_dump_sha256=before, activated_dump_sha256=snapshot,
             source_bytes=(root / Path(path).name).stat().st_size)

    save()
    try:
        db, path, local = prepare('baseline')
        require(lab.cli(state, 'config', 'initialize', '--file', path).returncode == 0, 'baseline initialize')
        baseline = json.loads(lab.sql(state, db, 'SELECT document FROM control.config_revisions;'))
        # Baseline defaults are product-derived; all discriminating mutations,
        # compact identities, expected sizes and durable comparisons are external.
        vectors = json.loads((SOURCE / 'experiments/configuration-inspection/jsonb-cases.json').read_text())['cases']
        for index, vector in enumerate(vectors):
            value = vector['compact']
            observed = lab.sql(state, db, "SELECT ($size_vector$" + value + "$size_vector$::jsonb)::text;")
            require(observed == vector['rendered'], 'PG rendering vector ' + str(index))
        case('frozen-renderings', count=len(vectors))
        policy = copy.deepcopy(baseline)
        # Ordinary literals, full-domain integers, nested default revisit bands,
        # escaping/Unicode and two independently pinned tiny fractions coexist.
        policy['settings']['monitor.schedule_jitter'] = 2.0030397744267762e-253
        policy['settings']['deriver.overlap.jaccard_min'] = 5e-324
        policy['settings']['deriver.genesis_floor'] = 18446744073709551615
        policy['settings']['seed.file'] = '/fixtures/é雪"\\a'
        for role in ['sync', 'monitor']:
            policy['settings'][role + '.endpoints.mode'] = 'fixed'
            policy['settings'][role + '.endpoints.urls'] = [f'wss://relay.example/{n:06}/' + 'x' * 21 for n in range(100000)]
        initial_size = len(rendered(json.loads(compact(policy), parse_float=Decimal)).encode())
        delta = CAP - initial_size
        require(1 < delta < 100000, 'boundary construction')
        for n in range(delta):
            policy['settings']['sync.endpoints.urls'][n] += 'x'
        require(len(rendered(json.loads(compact(policy), parse_float=Decimal)).encode()) == CAP, 'independent exact boundary')
        for name, offset in [('below', -1), ('exact', 0), ('above', 1), ('original', None)]:
            db, path, local = prepare(name)
            current = copy.deepcopy(policy)
            if offset == -1:
                current['settings']['sync.endpoints.urls'][0] = current['settings']['sync.endpoints.urls'][0][:-1]
            elif offset == 1:
                current['settings']['sync.endpoints.urls'][0] += 'x'
            elif offset is None:
                current = copy.deepcopy(baseline)
                for role in ['sync', 'monitor']:
                    current['settings'][role + '.endpoints.mode'] = 'fixed'
                    current['settings'][role + '.endpoints.urls'] = [f'wss://relay.example/{n:06}/' + 'x' * 22 for n in range(100000)]
            text = local.read_text()
            if offset is not None:
                text = text.replace('[monitor]\n', '[monitor]\nschedule_jitter=2.0030397744267762e-253\n')
                text = text.replace('[deriver]\n', '[deriver]\ngenesis_floor="18446744073709551615"\n')
                text += '\n[deriver.overlap]\njaccard_min=5e-324\n[seed]\nfile="/fixtures/é雪\\\"\\\\a"\n'
            for role in ['sync', 'monitor']:
                text += '\n[' + role + '.endpoints]\nmode="fixed"\nurls=' + compact(current['settings'][role + '.endpoints.urls']) + '\n'
            local.write_text(text)
            require(len(text.encode()) < CAP, 'source cap')
            if offset in [-1, 0]:
                accepted(db, path, name, current)
                continue
            raw, digest = identity(current)
            expected = len(rendered(json.loads(raw, parse_float=Decimal)).encode())
            observed = int(lab.sql(state, db, "SELECT octet_length($size_policy$" + raw.decode() + "$size_policy$::jsonb::text);"))
            require(len(raw) < CAP < expected == observed, 'over-limit oracle')
            before = lab.schema_digest(state, db)
            for action in ['validate', 'initialize', 'activate-initial']:
                extra = ['--request-id', '76' * 16, '--expected-revision', '1'] if action == 'activate-initial' else []
                result = execute(db, path, name + '-' + action, action, *extra, readonly=True)
                require(result.returncode == 3 and not result.stdout and 'persisted JSONB limit' in result.stderr, 'pre-acceptance refusal ' + action)
            case(name, compact_bytes=len(raw), jsonb_bytes=observed, policy_sha256=digest,
                 unchanged_dump_sha256=before, source_bytes=len(text.encode()))
        for name in ['baseline', 'below', 'exact', 'above', 'original']:
            db = 'bb_size_' + name
            require(lab.sql(state, db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control';") == '0', 'backend leak')
        report['status'] = 'PASS'
    except BaseException as error:
        report.update(status='FAIL', failure=str(error)); raise
    finally:
        save()
    print(json.dumps(dict(status=report['status'], cases=len(report['cases']))))


if __name__ == '__main__':
    main()
