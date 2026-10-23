#!/usr/bin/env python3
"""Config-only recovery on fresh checkpointed derivatives of pinned origins.

The existing transition owner performs origin exclusion, copies, native custody
and retained cleanup. Two deliberately privileged corruptions are fixture setup,
never ordinary product writes. Default: four typed successor checks. Opt-in ACK
profile: two initial refusals; no ACK ambiguity, full-window proof or GC claim.
"""
from datetime import datetime, timezone
import argparse
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


def ack_setup(pg, output, clone, directory, seed):
    mode = clone['name']; spool = directory/'store/producer-inputs-v1'
    before = schema.rows(pg,clone['database']); oracle.same(before,seed[0])
    mutation = dict(kind='privileged-isolated-fixture-corruption',mode=mode,
        statement=oracle.ack_statement(seed) if mode=='ack-ahead' else None,
        sql_output=None,before=before,after=None,inputs_before=history.inputs_at(directory/'store'),
        inputs_after=None,spool_before=history.census(spool),spool_after=None,quarantine=None)
    lab.write_json(output/(mode+'-mutation.json'),mutation)
    if mode == 'ack-ahead':
        (output/(mode+'-corruption.sql')).write_text(mutation['statement'])
        mutation['sql_output'] = lab.sql(pg,clone['database'],mutation['statement'])
    else:
        assert mode == 'missing-original'
        name = oracle.missing_original(seed); path = directory/('quarantined-'+name)
        assert not path.exists()
        data = (spool/name).read_bytes()
        mutation['quarantine'] = dict(name=name,path=str(path.relative_to(output)),hex=data.hex(),
            metadata=mutation['spool_before'][name])
        lab.write_json(output/(mode+'-mutation.json'),mutation)
        (spool/name).rename(path)
        assert history.census(path)['.'] == mutation['quarantine']['metadata']
    mutation.update(after=schema.rows(pg,clone['database']),
        inputs_after=history.inputs_at(directory/'store'),spool_after=history.census(spool))
    lab.write_json(output/(mode+'-mutation.json'),mutation)
    return mutation,oracle.ack_corruption_checked(mutation,seed,mode)


def run_sequence(pg, client, output, clone, directory, expected, report, *, origin, ack_refusals=False):
    profile = oracle.settings(ack_refusals)
    mode = clone['name']; assert mode in profile['clones'] and clone['origin'] == profile['origin']['name']
    seed = expected
    oracle.same(seed[0], origin[0])
    assert seed[1:] == origin[1:]
    assert len(seed[0]) == 77
    helper = Path(__file__).with_name('installed_cycle_client.py')
    target = output / helper.name
    if not target.exists(): shutil.copy2(helper, target)
    assert history.digest(target) == history.digest(helper)
    configure(pg, client, output, clone['config'],ack=ack_refusals)
    mutation = None
    if mode != 'positive':
        # No CLI/native owner exists on this freshly copied root. Observe the
        # actual database exclusion before the explicitly recorded corruption.
        absent = history.observe_backend_absence(pg, clone['database'], output, mode + '-operator')
        assert absent['status'] == 'OBSERVED_ABSENT' and schema.absence(pg, clone['database']) == []
        if ack_refusals:
            mutation,expected = ack_setup(pg,output,clone,directory,seed)
        else:
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
            expected = mutation['after'], seed[1], seed[2]
        assert history.catalog(pg, clone['database']) == clone['catalog_before']
    predecessor = None
    for step in (('recover', 'retry') if mode == 'positive' else ('refuse',)):
        case = dict(name=mode+'-'+step, clone=mode, origin=clone['origin'],
            operation='recover-initial' if ack_refusals else 'recover',
            predecessor=predecessor, database=clone['database'], config=clone['config'], status='RUNNING')
        report['cases'].append(case); lab.write_json(output / 'result.json', report)
        diagnostic = None

        def verify(context, original):
            nonlocal diagnostic
            if mode != 'positive' and not ack_refusals:
                # Bind a generic adapter Backend refusal to the exact validator
                # error on its newly claimed, already excluded actual backend.
                window = context['server_window']
                bounds = [datetime.fromtimestamp(window[k]/1_000_000, timezone.utc).isoformat()
                    for k in ('before_us', 'after_us')]
                answer = lab.docker('logs', '--timestamps', '--since', bounds[0], '--until', bounds[1],
                    pg['postgres'], check=False, timeout=5)
                diagnostic = dict(returncode=answer.returncode, stdout=answer.stdout, stderr=answer.stderr)
                lab.write_json(output / (case['name'] + '-diagnostic.json'), diagnostic)
            return oracle.verify(context, original, mutation=mutation, diagnostic=diagnostic,ack_refusals=ack_refusals)

        def controls(context, original):
            return oracle.negative_controls(context, original, mutation=mutation, diagnostic=diagnostic,ack_refusals=ack_refusals)

        expected = installed_cycle.execute(pg, client, output, case, directory, expected,
            clone['catalog_before'], seed, checks=(verify, controls, partial(oracle.process_closed,ack_refusals=ack_refusals)))
        if ack_refusals:
            final = history.census(directory/'store/producer-inputs-v1')
            lab.write_json(output/(mode+'-spool-final.json'),final)
            assert final == mutation['spool_after']
            if mutation['quarantine'] is not None:
                q = mutation['quarantine']; path = output/q['path']
                assert history.census(path)['.'] == q['metadata'] and path.read_bytes().hex() == q['hex']
        case['catalog_after'] = history.catalog(pg, clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = case['name']; lab.write_json(output / 'result.json', report)
    return predecessor


def run(root, build, *, ack_refusals=False):
    # Pure file/oracle preflight precedes the existing resource owner. No
    # structural PG checksum alone can qualify the copied native science.
    profile = oracle.settings(ack_refusals); pin = profile['origin']
    origin = oracle.origin_checked(root.parent/pin['run']/pin['subdirectory'],ack_refusals=ack_refusals)
    suite = SUITE
    if ack_refusals:
        suite = transition.Suite(directory=profile['directory'],label=profile['label'],status=profile['status'],
            client_prefix='bigbrotr-ack-refusals-',database_prefix=profile['prefix'],
            clones=profile['clones'],cases=profile['cases'],
            scope='Two initial config-only refusals on excluded derivatives of qualified hole recovery/retry: '
                  'native A7>C6 and missing exact post-hole original5; all science retained',
            not_run=('Native ACK ambiguity/post-ACK loss','Actual full-window proof-only','GC/capacity/backup'))
    return transition.run(root, build, suite=suite,
        sequence=partial(run_sequence, origin=origin,ack_refusals=ack_refusals),
        origin_specs=(pin,), origin_for_clone={name:pin['name'] for name in profile['clones']},
        extra_grants=() if ack_refusals else GRANTS,
        producer_progress=True,producer_ack=ack_refusals,preparation_hole=ack_refusals)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path); parser.add_argument('build',type=Path)
    parser.add_argument('--ack-refusals',action='store_true')
    args = parser.parse_args()
    run(args.root.resolve(strict=True),args.build.resolve(strict=True),ack_refusals=args.ack_refusals)
