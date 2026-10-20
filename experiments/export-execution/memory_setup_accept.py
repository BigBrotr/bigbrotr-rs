#!/usr/bin/env python3
"""Root-only follow-up: accept once, audit raw state, prepare measurement inputs.

The root must first authenticate the prepared setup and supply the absolute
cgroup path visible to the future measurement observer. This descriptor does not
reserve a cgroup: root must provision and authenticate its actual fresh identity
before measurement. This command never starts Serve or the measurement runner.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import time
import tomllib

# This import sets the explicit inert oracle paths; its main guard stays false.
from memory_setup import immutable
from memory_setup_io import IO, pinned, regular, require, sha, write
from memory_setup_oracle import acceptance


def run(root, group):
    os.umask(0o077)
    require(root.is_absolute() and root.resolve() == root and group.is_absolute(), 'explicit root/cgroup paths')
    prepared = json.loads(regular(root / 'prepared.json'))
    setup_result = json.loads(regular(root / 'result.json'))
    require(prepared['status'] == setup_result['status'] == 'PREPARED_FOR_ROOT_ACCEPTANCE',
            'known prepared result only')
    spec = json.loads(regular(root / 'input.json'))
    require(spec['root'] == str(root), 'same inventoried setup root')
    for key in ('application', 'probe', 'ingest_probe', 'psql', 'source_manifest', 'inventory', 'recipe'):
        pinned(spec[key], executable=key in ('application', 'probe', 'ingest_probe', 'psql'))
    case_path = root / 'accept.json'
    require(sha(regular(case_path, 65536)) == setup_result['accept_sha256'], 'exact prepared acceptance descriptor')
    case = json.loads(case_path.read_text())
    profiles = prepared['profiles']
    require(set(profiles) == {'destination', 'publisher', 'ordinary'}
            and profiles == json.loads(regular(root / 'profiles.json')), 'exact prepared profile inventory')
    for name, pin in profiles.items():
        require(pin['path'] == str(root / (name + '.toml'))
                and Path(pin['path']).stat().st_mode & 0o777 == 0o400, 'original frozen profile paths')
        pinned(pin)
    destination = pinned(profiles['destination'])
    require(destination == pinned(spec['config']), 'destination profile is the original supplied recipe')
    config = tomllib.loads(destination.decode())
    output = root / 'accept-run'
    output.mkdir(mode=0o700)
    io = IO(output, spec, config)
    def interrupted(_signal, _frame):
        raise InterruptedError('owned acceptance signal')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        io.backends('before', absent=True)
        before = io.capture('before')
        require(before == json.loads((root / 'granted.json').read_text()), 'all original 75 tables and catalog unchanged')
        require(immutable(root) == prepared['files'], 'same physical native/data/state/destination')
        # Five real password operations may have filled the account bucket. Wait
        # only on database time; no sixth denial, bucket rewrite or policy change.
        buckets = [row for row in before['control']['login_buckets'] if row['kind'] == 'account']
        require(len(buckets) == 1 and 1 <= buckets[0]['attempts'] <= 5, 'actual finite login bucket')
        target = buckets[0]['window_started_at_us'] + 60_000_000
        observations = []
        while True:
            now = int(io.sql(config['postgres']['control']['database'],
                "SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint;"))
            observations.append(dict(server_now_us=now, window_ends_us=target))
            if now >= target:
                break
            require(len(observations) <= 61 and io.remaining() > 5, 'bounded natural login-window observation')
            time.sleep(min(1, (target-now)/1_000_000))
        write(output / 'login-window.json', observations)
        # Pin life remains clamped by Control's 5s lease inside this operation.
        # No refresher here can extend a pin or modify that production deadline.
        raw = io.run('accept', [spec['probe']['path'], 'accept', str(case_path)], seconds=120)
        events = [json.loads(line) for line in raw.splitlines()]
        io.backends('after', absent=True)
        after = io.capture('after')
        require(after['catalog'] == before['catalog'] and immutable(root) == prepared['files'],
                'complete original publication preserved through acceptance')
        job = acceptance(before['control'], after['control'], case, prepared['selection'], events)
        keys = {'username', 'password_ref', 'command_key', 'command'}
        measure = {key: value for key, value in case.items() if key not in keys}
        scratch = root / 'measure-scratch'
        scratch.mkdir(mode=0o700)
        measure.update(format='bigbrotr-export-measure/1', job=job,
            event_id=prepared['selection']['query']['event_id'], scratch=str(scratch),
            ledger=str(root / 'measure-ledger.jsonl'))
        measure_path = root / 'case.json'
        write(measure_path, measure)
        science = dict(format='bigbrotr-export-memory-science/1',
            event=dict(kind='frozen-archive-recipe', path=spec['recipe']['path'], sha256=spec['recipe']['sha256']),
            query=prepared['selection']['query'], catalog=prepared['selection']['catalog'],
            frontier_revision=prepared['selection']['frontier_revision'],
            accepted_revisions=dict(configuration=case['command']['expected_config_revision'],
                                    policy=case['command']['expected_policy_revision']))
        science_path = root / 'science.json'
        write(science_path, science)
        runner = dict(format='bigbrotr-export-memory-run/1', probe=spec['probe']['path'],
            probe_sha256=spec['probe']['sha256'], case=str(measure_path), case_sha256=sha(measure_path.read_bytes()),
            science=str(science_path), science_sha256=sha(science_path.read_bytes()), cgroup=str(group))
        write(root / 'run.json', runner)
        write(output / 'result.json', dict(status='ACCEPTED_UNRESERVED_INPUTS_PREPARED', job=job,
            measurement='NOT_RUN', cgroup='explicit root-supplied path; freshness/custody not inferred here',
            reader_ttl='requested30s, clamped to Control5s; refresh does not extend pin',
            runtime_claims='only this actual acceptance result after independent raw-state/custody checks',
            seconds=120-io.remaining(), run_sha256=sha((root / 'run.json').read_bytes())))
    except BaseException as error:
        io.failure_custody()
        write(output / 'failure.json', dict(status='UNKNOWN_PRESERVED', error=type(error).__name__,
            job='may exist; never rerun acceptance with a new key', measurement='NOT_RUN'))
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('prepared_root', type=Path)
    parser.add_argument('measurement_cgroup', type=Path)
    args = parser.parse_args()
    run(args.prepared_root, args.measurement_cgroup)
