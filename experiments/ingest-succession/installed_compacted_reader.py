#!/usr/bin/env python3
"""Bounded synthetic post-GC initial-reader fixture, not product reclamation.

Four excluded clones first execute actual abandonment and retirement. One raw
test-only synced batch then installs the literal floor and deletes seven exact
receipt keys. Every input and removed byte remains retained as fixture evidence.
The existing transition owner alone owns engines, cloning, custody and cleanup.
"""
import argparse
from datetime import datetime, timezone
from functools import partial
import os
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
import transition
import installed_cycle
from installed_family import configure
import installed_compacted_reader_oracle as oracle

history, schema, lab = transition.history, transition.schema, transition.lab
SUITE = transition.Suite(directory=oracle.DIRECTORY,label=oracle.LABEL,status=oracle.STATUS,
    client_prefix='bigbrotr-compacted-reader-',database_prefix='bb_compacted_reader_',
    clones=oracle.CLONES,cases=oracle.CASES,
    scope='Synthetic post-GC reader fixture after authentic installed abandonment and retirement: '
          'full residual originals, absent-original exact retry, incompatible typed head/floor digest/missing pending refusals',
    not_run=oracle.NOT_RUN)


def native_mutation(pg,client,output,clone,directory,expected,seed,mode):
    name = clone['name']; label = name+'-synthetic-'+mode
    # This all-session observer connects to postgres, never to the target DB.
    transition.observe_origin_absence(pg,clone['database'],output,label)
    oracle.terminal_checked(expected[0],seed)
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    assert history.inputs_at(directory/'store') == seed[2]
    floor = oracle.floor_bytes(seed,name).hex()
    before = history.dump(client,output,label+'-before',directory)
    assert before == expected[1]
    mutation = dict(kind='privileged-synthetic-post-GC-reader-fixture',mode=mode,clone=name,
        floor=floor,before=before,control=expected[0],inputs=expected[2],after=None,response=None)
    lab.write_json(output/(label+'.json'),mutation)
    response,_ = history.capture(client,output,label,'compacted-reader-fixture',
        '/lab/'+name+'/store',mode,floor)
    mutation['response'] = dict(returncode=response.returncode,stdout=response.stdout,stderr=response.stderr)
    lab.write_json(output/(label+'.json'),mutation)
    # No fresh native open is allowed after a failed/unknown mutator process.
    assert response.returncode == 0 and response.stderr == ''
    wanted = oracle.compacted_native(seed,name,missing=mode == 'missing-pending')
    oracle.mutation_checked(mutation,seed,expected,wanted,complete=False)
    mutation['after'] = history.dump(client,output,label+'-after',directory)
    oracle.same(schema.rows(pg,clone['database']),expected[0])
    assert history.inputs_at(directory/'store') == seed[2]
    lab.write_json(output/(label+'.json'),mutation)
    oracle.mutation_checked(mutation,seed,expected,wanted)
    return expected[0],wanted,expected[2]


def incompatible_head(pg,output,clone,expected,seed):
    label = clone['name']+'-head-mutation'
    transition.observe_origin_absence(pg,clone['database'],output,label)
    before = schema.rows(pg,clone['database']); oracle.same(before,expected[0])
    statement = oracle.retained.ack_statement(seed)
    mutation = dict(kind='privileged-isolated-valid-head-corruption',before=before,
        statement=statement,sql_output=None,after=None)
    lab.write_json(output/(label+'.json'),mutation)
    (output/(label+'.sql')).write_text(statement)
    mutation['sql_output'] = lab.sql(pg,clone['database'],statement)
    mutation['after'] = schema.rows(pg,clone['database'])
    lab.write_json(output/(label+'.json'),mutation)
    after = oracle.head_mutation_checked(mutation,expected[0],seed)
    return after,expected[1],expected[2]


def quarantine(output,directory,expected,seed):
    assert expected[2] == seed[2]
    spool = directory/'store/producer-inputs-v1'; target = directory/'quarantined-originals'
    target.mkdir(mode=0o700)
    before = history.census(spool)
    record = dict(kind='reversible-fixture-original-quarantine',before=before,after=None,
        path=str(target.relative_to(output)),files={},quarantine_after=None,status='INCOMPLETE')
    for name in sorted(seed[2]):
        raw = oracle.retained.regular_read(spool/name)
        record['files'][name] = dict(hex=raw.hex(),metadata=before[name])
    lab.write_json(output/'positive-quarantine.json',record)
    for name in sorted(seed[2]): (spool/name).rename(target/name)
    for path in (spool,target,directory):
        fd = os.open(path,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    record.update(after=history.census(spool),quarantine_after=history.census(target),status='RETAINED')
    lab.write_json(output/'positive-quarantine.json',record)
    oracle.quarantine_checked(record,seed)
    assert history.inputs_at(directory/'store') == {}
    return expected[0],expected[1],{}


def run_sequence(pg,client,output,clone,directory,expected,report,*,origin):
    oracle.same(expected[0],origin[0]); assert expected[1:] == origin[1:]
    seed = origin; oracle.known(seed)
    assert clone['name'] in oracle.CLONES and clone['origin'] == 'initial'
    assert tuple(g['statement'] for g in clone['grants']) == oracle.GRANTS
    assert all(g['stdout'] == '' for g in clone['grants'])
    oracle.catalog_checked(report['origins']['initial']['catalog'],clone['catalog_before'])
    helper = Path(__file__).with_name('installed_cycle_client.py'); target = output/helper.name
    if not target.exists(): shutil.copy2(helper,target)
    assert history.digest(helper) == history.digest(target)
    configure(pg,client,output,clone['config'],ack=True)
    predecessor = None
    for step in oracle.STEPS[clone['name']]:
        if step in ('residual','refuse'):
            oracle.terminal_checked(expected[0],seed)
            lab.write_json(output/(clone['name']+'-terminal-seed.json'),dict(control=expected[0],native=expected[1],inputs=expected[2]))
            expected = native_mutation(pg,client,output,clone,directory,expected,seed,'install')
            if clone['name'] == 'incompatible-head': expected = incompatible_head(pg,output,clone,expected,seed)
            if clone['name'] == 'missing-pending':
                expected = native_mutation(pg,client,output,clone,directory,expected,seed,'missing-pending')
        elif step == 'absent': expected = quarantine(output,directory,expected,seed)
        case = dict(name=clone['name']+'-'+step,clone=clone['name'],origin='initial',
            operation='abandon-initial' if step == 'abandon' else 'retire-initial',predecessor=predecessor,
            database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        diagnostic = None
        def verify(context,original):
            nonlocal diagnostic
            if step == 'refuse' and clone['name'] == 'floor-digest':
                window = context['server_window']
                bounds = [datetime.fromtimestamp(window[k]/1_000_000,timezone.utc).isoformat() for k in ('before_us','after_us')]
                reply = lab.docker('logs','--timestamps','--since',bounds[0],'--until',bounds[1],pg['postgres'],check=False,timeout=5)
                diagnostic = dict(returncode=reply.returncode,stdout=reply.stdout,stderr=reply.stderr)
                lab.write_json(output/(case['name']+'-diagnostic.json'),diagnostic)
            return oracle.verify(context,original,expected,diagnostic=diagnostic)
        def controls(context,original):
            return oracle.negative_controls(context,original,expected,diagnostic=diagnostic)
        expected = installed_cycle.execute(pg,client,output,case,directory,expected,
            clone['catalog_before'],seed,checks=(verify,controls,oracle.process_closed))
        if step == 'retire': oracle.terminal_checked(expected[0],seed)
        if step == 'absent':
            q = transition.read(output/'positive-quarantine.json')
            assert history.census(output/q['path']) == q['quarantine_after']
            assert history.census(directory/'store/producer-inputs-v1') == q['after']
        case['catalog_after'] = history.catalog(pg,clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run(root,build,product_source):
    origin = oracle.retained.origin_checked(root.parent/ORIGIN['run']/ORIGIN['subdirectory'],ack_refusals=True)
    oracle.known(origin)
    return transition.run(root,build,suite=SUITE,sequence=partial(run_sequence,origin=origin),
        origin_specs=(ORIGIN,),origin_for_clone={name:'initial' for name in oracle.CLONES},
        extra_grants=oracle.GRANTS,producer_progress=True,producer_ack=True,preparation_hole=True,
        compacted_reader=True,product_source=product_source)


ORIGIN = oracle.ORIGIN
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path); parser.add_argument('build',type=Path)
    parser.add_argument('--product-source',required=True,help='Exact reviewed and qualified integrated build revision')
    args = parser.parse_args()
    run(args.root.resolve(strict=True),args.build.resolve(strict=True),args.product_source)
