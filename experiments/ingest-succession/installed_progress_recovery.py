#!/usr/bin/env python3
"""Four actual config-only processes on three fresh checkpointed derivatives.

The existing transition owner performs origin exclusion, copies, native custody
and retained cleanup. Two deliberately privileged corruptions are fixture setup,
never ordinary product writes. This does not qualify ACK, GC or interrupted COMMIT.
"""
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
import transition
import installed_cycle
from installed_family import configure
import installed_progress_recovery_oracle as oracle
from installed_progress_replay import PROGRESS_GRANTS

history, schema, lab = transition.history, transition.schema, transition.lab
SUITE = transition.Suite(directory=oracle.DIRECTORY, label=oracle.LABEL, status=oracle.STATUS,
    client_prefix='bigbrotr-progress-recovery-', database_prefix=oracle.PREFIX,
    clones=oracle.CLONES, cases=oracle.CASES,
    scope='One checkpointed successor3 actual recovery and exact retry; separate missing closure '
          'and foreign historical issuer refusals; ACK0 and all original detail retained',
    not_run=('Initial/activated recovery contexts and separate no-checkpoint origin',
        'Missing native fact injection: existing qualified corruption probe is fixed to epoch2',
        'COMMIT reply loss, ACK/credit/GC, source closure, workflow completion and capacity/RSS'))
GRANTS = (*installed_cycle.GRANTS, PROGRESS_GRANTS)


def run_sequence(pg, client, output, clone, directory, expected, report, *, origin):
    mode = clone['name']; assert mode in oracle.CLONES and clone['origin'] == 'activated'
    seed = expected
    oracle.same(seed[0], origin[0])
    assert seed[1:] == origin[1:]
    assert len(seed[0]) == 77
    helper = Path(__file__).with_name('installed_cycle_client.py')
    target = output / helper.name
    if not target.exists(): shutil.copy2(helper, target)
    assert history.digest(target) == history.digest(helper)
    configure(pg, client, output, clone['config'])
    mutation = None
    if mode != 'positive':
        # No CLI/native owner exists on this freshly copied root. Observe the
        # actual database exclusion before the explicitly recorded corruption.
        absent = history.observe_backend_absence(pg, clone['database'], output, mode + '-operator')
        assert absent['status'] == 'OBSERVED_ABSENT' and schema.absence(pg, clone['database']) == []
        before = schema.rows(pg, clone['database']); oracle.same(seed[0], before)
        statement = oracle.CORRUPTIONS[mode]
        (output / (mode + '-corruption.sql')).write_text(statement)
        mutation = dict(kind='privileged-isolated-fixture-corruption', mode=mode,
            statement=statement, sql_output=None, before=before, after=None)
        lab.write_json(output / (mode + '-mutation.json'), mutation)
        mutation['sql_output'] = lab.sql(pg, clone['database'], statement)
        mutation['after'] = schema.rows(pg, clone['database'])
        lab.write_json(output / (mode + '-mutation.json'), mutation)
        oracle.corruption_checked(mutation, seed, mode)
        assert history.catalog(pg, clone['database']) == clone['catalog_before']
        expected = mutation['after'], seed[1], seed[2]
    predecessor = None
    for step in (('recover', 'retry') if mode == 'positive' else ('refuse',)):
        case = dict(name=mode+'-'+step, clone=mode, origin='activated', operation='recover',
            predecessor=predecessor, database=clone['database'], config=clone['config'], status='RUNNING')
        report['cases'].append(case); lab.write_json(output / 'result.json', report)
        diagnostic = None

        def verify(context, original):
            nonlocal diagnostic
            if mode != 'positive':
                # Bind a generic adapter Backend refusal to the exact validator
                # error on its newly claimed, already excluded actual backend.
                window = context['server_window']
                bounds = [datetime.fromtimestamp(window[k]/1_000_000, timezone.utc).isoformat()
                    for k in ('before_us', 'after_us')]
                answer = lab.docker('logs', '--timestamps', '--since', bounds[0], '--until', bounds[1],
                    pg['postgres'], check=False, timeout=5)
                diagnostic = dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr)
                lab.write_json(output / (case['name'] + '-diagnostic.json'), diagnostic)
            return oracle.verify(context, original, mutation=mutation, diagnostic=diagnostic)

        def controls(context, original):
            return oracle.negative_controls(context, original, mutation=mutation, diagnostic=diagnostic)

        expected = installed_cycle.execute(pg, client, output, case, directory, expected,
            clone['catalog_before'], seed, checks=(verify, controls, oracle.process_closed))
        case['catalog_after'] = history.catalog(pg, clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = case['name']; lab.write_json(output / 'result.json', report)
    return predecessor


def run(root, build):
    # Pure file/oracle preflight precedes the existing resource owner. No
    # structural PG checksum alone can qualify the copied native science.
    origin = oracle.origin_checked(root.parent / oracle.ORIGIN['run'] / oracle.ORIGIN['subdirectory'])
    return transition.run(root, build, suite=SUITE, sequence=partial(run_sequence, origin=origin),
        origin_specs=(oracle.ORIGIN,), origin_for_clone={name:'activated' for name in oracle.CLONES},
        extra_grants=GRANTS, producer_progress=True)


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
