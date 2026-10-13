"""Bounded raw relational retirement checks with real application Control custody.

SQL does not prove native exclusion. Every app transaction rolls back or refuses;
only two explicit isolated operator deletions precede the missing-detail cases.
"""
import copy
import json
from pathlib import Path
import shutil
import subprocess
import time

import lab
import activated_recovery as recovery
import activated_retirement as retirement
from activated_retirement_guards_oracle import CASES, MODE, verify, verify_case, negative_controls
from initial_empty_guards import _setup, _absence, _literal
from initial_gap import configuration
from initial_history import capture, inputs_at, observe_backend_absence, server_clock
from initial_succession import census, digest


def privileges(pg, database):
    return json.loads(lab.sql(pg, database, "SELECT json_object_agg(c.relname, "
        "has_table_privilege('bigbrotr_control',c.oid,'TRUNCATE')) FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='control' AND c.relkind='r'"))


def statement_set(before):
    intent, = before['initial_ingest_successions']
    source, = [r for r in before['source_streams'] if r['owner_epoch'] == 2]
    resource, slot, dataset, role = map(_literal, (intent['owner_resource_id'], intent['producer_slot_id'],
        intent['dataset_id'], intent['target_role_instance_id']))
    closure = _literal(source['closure_digest'])
    bad = bytearray.fromhex(source['closure_digest'][2:])
    bad[0] ^= 1
    bad = "decode('" + bad.hex() + "','hex')"
    predicates = dict(producer=f'producer_slot_id={slot} AND producer_epoch=2',
        domains=f'dataset_id={dataset} AND owner_resource_id={resource} AND owner_epoch=2',
        owner=f'resource_id={resource} AND epoch=2', role=f'role_instance_id={role}')
    producer = f"UPDATE control.producer_epochs SET state='retired',retired_at_us=:fixture_time,closure_digest={closure} WHERE {predicates['producer']};\n"
    domains = f"UPDATE control.allocation_domains SET state='closed',closed_at_us=:fixture_time,closure_digest={closure} WHERE {predicates['domains']}"
    owner = f"UPDATE control.owner_epochs SET state='released',released_at_us=:fixture_time,exclusion_method='storage_fenced',exclusion_digest={closure} WHERE {predicates['owner']};\n"
    retire_role = f"UPDATE control.role_instances SET retired_at_us=:fixture_time WHERE {predicates['role']};\n"
    complete = producer + domains + ';\n' + owner + retire_role
    output = {}
    for mode in CASES:
        prefix, identity = _setup(before, intent, MODE + '-' + mode, 'bigbrotr_control')
        def state(stage):
            tables = ',\n'.join("('" + name + "',(SELECT coalesce(jsonb_agg(to_jsonb(t)||jsonb_build_object('xmin',t.xmin::text)),'[]'::jsonb) FROM control." + name + ' t))' for name in sorted(before))
            return f"""SELECT jsonb_build_object('event','state','stage','{stage}',
 'transaction_id',pg_current_xact_id()::text,'observed_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,
 'rows',(SELECT jsonb_object_agg(table_name,table_rows) FROM (VALUES {tables}) full_state(table_name,table_rows)));
"""
        def classify(stage):
            return f"SELECT json_build_object('event','classified','stage','{stage}','terminal_at',control.require_activated_initial_retirement({resource}));\n"
        sql = prefix + state('baseline')
        if mode == 'legacy-grants':
            current_resource = _literal(intent['prepared_by_resource'])
            current_role = "decode('" + identity['role_instance'] + "','hex')"
            old_owner, = [r for r in before['owner_epochs'] if r['role'] == 'ingest' and r['epoch'] == 1]
            sql += f"""UPDATE control.owner_epochs SET state='released',released_at_us=:fixture_time,
 exclusion_method='storage_fenced',exclusion_digest=sha256(decode('{identity['incarnation']}','hex'))
 WHERE resource_id={current_resource} AND epoch={identity['control_epoch']};
UPDATE control.role_instances SET retired_at_us=:fixture_time WHERE role_instance_id={current_role};
UPDATE control.role_instances SET created_at_us=created_at_us WHERE role_instance_id={_literal(old_owner['role_instance_id'])};
""" + state('legacy') + "SET CONSTRAINTS ALL IMMEDIATE;\nROLLBACK;\nSELECT json_build_object('event','rolled-back');\n"
            output[mode] = sql
            continue
        sql += classify('ready')
        if mode.startswith('missing-'):
            output[mode] = sql + "ROLLBACK;\n"
            continue
        if mode == 'ready-rollback':
            sql += "ROLLBACK;\nSELECT json_build_object('event','rolled-back');\n"
        elif mode == 'owner-only':
            sql += owner
        elif mode == 'wrong-producer-digest':
            sql += producer.replace(closure, bad)
        elif mode == 'immediate-before':
            sql += "SET CONSTRAINTS ALL IMMEDIATE;\nSELECT json_build_object('event','immediate');\n" + producer
        elif mode in ('producer-only', 'domain-only', 'role-only'):
            mutation = {'producer-only':producer, 'domain-only':domains + f" AND dictionary_kind=1;\n", 'role-only':retire_role}[mode]
            sql += mutation + state('partial') + "SELECT json_build_object('event','before-commit');\nCOMMIT;\n"
        else:
            mutation = complete
            if mode == 'wrong-common-time':
                mutation = mutation.replace('SET retired_at_us=:fixture_time WHERE', 'SET retired_at_us=:fixture_time+1 WHERE')
            elif mode == 'wrong-domain-digest':
                mutation = producer + domains.replace(closure, bad) + ';\n' + owner + retire_role
            sql += mutation + state('terminal')
            if mode in ('wrong-common-time', 'wrong-domain-digest'):
                sql += "SELECT json_build_object('event','before-commit');\nCOMMIT;\n"
            else:
                sql += 'SET CONSTRAINTS ALL IMMEDIATE;\n' + classify('complete') + state('checked')
                if mode == 'complete-rollback':
                    sql += "ROLLBACK;\nSELECT json_build_object('event','rolled-back');\n"
                else:
                    sql += {
                        'role-created-after-immediate': f"UPDATE control.role_instances SET created_at_us=created_at_us+1 WHERE {predicates['role']};\n",
                        'role-time-after-immediate': f"UPDATE control.role_instances SET retired_at_us=retired_at_us+1 WHERE {predicates['role']};\n",
                        'role-delete': f"DELETE FROM control.role_instances WHERE {predicates['role']};\n",
                        'role-truncate': 'TRUNCATE control.role_instances;\n',
                        'role-truncate-cascade': 'TRUNCATE control.role_instances CASCADE;\n',
                    }[mode]
        output[mode] = sql
    return output


def operator_sql(mode):
    if mode == 'missing-copy':
        table, predicate = 'producer_outcomes', 'producer_epoch=2 AND request_sequence=2'
    else:
        assert mode == 'missing-gap'
        table, predicate = 'acquisition_gaps', 'producer_epoch=2'
    return ("BEGIN;\nSET LOCAL statement_timeout='10s';\nSET LOCAL lock_timeout='5s';\n"
        "SET LOCAL session_replication_role=replica;\nDELETE FROM control." + table + ' WHERE ' + predicate +
        " RETURNING (to_jsonb(" + table + ")||jsonb_build_object('xmin',xmin::text))::text;\nCOMMIT;\n")


def execute(pg, database, output, rows, directory, mode, sql, shared, operator=None, legacy=None):
    label = MODE + '-' + mode
    response = dict(returncode=None, stdout='', stderr='', outcome='OUTCOME_UNKNOWN')
    case = dict(mode=mode, database=database, statement=sql, statement_sha256='', response=response,
        events=[], server_window={}, capture_errors=[], backend_absence=None, operator=operator, legacy=legacy)
    path = output / (label + '-verification.json')
    (output / (label + '.sql')).write_text(sql)
    case['statement_sha256'] = digest(output / (label + '.sql'))
    case.update(before=rows(database), catalog_before=retirement.catalog(pg, database),
        privileges_before=privileges(pg,database), files_before=census(directory/'store'),
        inputs_before=inputs_at(directory/'store'), contents_before=recovery.contents_at(directory/'store'))
    case['server_window']['before_us'] = server_clock(pg,database)
    command = ['exec','-i','--env','PGCONNECT_TIMEOUT=5',pg['postgres'],'psql','-X','-A','-t','-q',
        '--no-password','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-U','bigbrotr_control','-d',database]
    case['command'] = command
    started = time.monotonic()
    try:
        answer = lab.docker(*command,data=sql,check=False,timeout=30)
        response.update(returncode=answer.returncode,stdout=answer.stdout,stderr=answer.stderr,outcome='PROCESS_EXITED')
    except subprocess.TimeoutExpired as error:
        def text(raw):
            return raw.decode(errors='replace') if isinstance(raw,bytes) else raw or ''
        response.update(stdout=text(error.stdout),stderr=text(error.stderr),outcome='UNKNOWN_TIMEOUT')
    finally:
        response['seconds'] = time.monotonic()-started
        lab.write_json(output/(label+'-response.json'),response)
        lab.write_json(path,case)
        try:
            case['events'] = [json.loads(line) for line in response['stdout'].splitlines() if line.startswith('{')]
        except (ValueError,TypeError) as error:
            case['capture_errors'].append('events:'+type(error).__name__)
        backends = [e for e in case['events'] if e.get('event')=='backend']
        for name,read in [('clock',lambda:server_clock(pg,database)),('after',lambda:rows(database)),
                ('catalog_after',lambda:retirement.catalog(pg,database)),('privileges_after',lambda:privileges(pg,database)),
                ('files_after',lambda:census(directory/'store')),('inputs_after',lambda:inputs_at(directory/'store')),
                ('contents_after',lambda:recovery.contents_at(directory/'store')),
                ('backend_absence',lambda:_absence(pg,database,'bigbrotr_control',backends[0] if len(backends)==1 else None))]:
            try:
                value=read()
                if name=='clock':
                    case['server_window']['after_us']=value
                else:
                    case[name]=value
                lab.write_json(output/(label+'-'+name+'.json'),value)
            except Exception as error:
                case['capture_errors'].append(name+':'+type(error).__name__)
            lab.write_json(path,case)
    verify_case(case,**shared)
    return case


def run_all(pg, client, output, rows, clone):
    spec=retirement.SOURCES['off']
    basis=output.parent.parent/spec['run']/'bindings'
    store=basis/spec['prefix']/'store'
    frozen={'result.json':spec['result'],spec['sql_file']:spec['sql'],spec['native_file']:spec['native'],spec['verification_file']:spec['verification']}
    origin=context=None
    owned=[]
    try:
        assert all(digest(basis/name)==value for name,value in frozen.items())
        retained=json.loads((basis/'result.json').read_text())
        assert all(retained[k]==spec[k] for k in ('source','owner','status'))
        assert not any(retained[k] for k in ('cleanup_errors','unrelated_changed','missing_owned'))
        containers=json.loads(lab.docker('inspect',*lab.docker('ps','-aq').stdout.split()).stdout)
        old,=[c for c in containers if c['Id']==retained['client']['id']]
        assert old['Config']['Labels'][retained['label']]==spec['owner']
        assert not old['State']['Running'] and not old['State']['Pid'] and not old['State']['OOMKilled']
        assert not any(c['State']['Running'] and any(str(basis) in m.get('Source','') for m in c['Mounts']) for c in containers)
        absent=observe_backend_absence(pg,spec['database'],output,MODE+'-original')
        original=rows(spec['database'])
        assert original==json.loads((basis/spec['sql_file']).read_text())
        seed=dict(name='off',verification=json.loads((basis/spec['verification_file']).read_text()),control=original,
            native=json.loads((basis/spec['native_file']).read_text()),inputs=inputs_at(store),contents=recovery.contents_at(store),
            catalog=retirement.catalog(pg,spec['database']))
        origin=dict(seed=seed,files=census(store),frozen=frozen,backend_absence=absent,client_id=old['Id'],client_state=old['State'],label=retained['label'])
        lab.write_json(output/(MODE+'-original.json'),origin)
        context=dict(seed=seed,clones=[],cases=[])
        for suffix, modes in [('normal',[m for m in CASES if not m.startswith('missing-')]),
                ('copy',['missing-copy']),('gap',['missing-gap'])]:
            label=MODE+'-'+suffix
            database=clone(label,spec['database'])
            assert rows(database)==original
            directory=output/label
            directory.mkdir(mode=0o700)
            shutil.copytree(store,directory/'store')
            observe_backend_absence(pg,database,output,label+'-before')
            answer,_=capture(client,output,label+'-before-dump','dump-state','/lab/'+label+'/store')
            assert answer.returncode==0 and not answer.stderr
            native=json.loads(answer.stdout)
            assert native==seed['native']
            files=census(directory/'store')
            config=configuration(pg,client,output,database,directory,original)
            upgrade=retirement.upgrade(pg,client,output,database,config,label,rows)
            setup=dict(migration_sha256=digest(Path(__file__).resolve().parents[2]/'migrations/postgres/0037_activated_initial_retirement.sql'),
                migration=upgrade,catalog_before=retirement.catalog(pg,database),catalog_after=retirement.catalog(pg,database))
            prior_privileges=privileges(pg,database)
            grant="GRANT DELETE ON control.role_instances TO bigbrotr_control;\nGRANT TRUNCATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control;\n"
            revoke='REVOKE DELETE ON control.role_instances FROM bigbrotr_control;\nREVOKE TRUNCATE ON '+','.join('control.'+t for t in sorted(prior_privileges) if not prior_privileges[t])+' FROM bigbrotr_control;\n'
            item=dict(database=database,setup=setup,native=native,files=files,baseline=rows(database),grants=dict(statement=grant,
                before=retirement.catalog(pg,database),privileges_before=prior_privileges),restored=None,legacy_restore=None)
            owned.append((item,revoke))
            context['clones'].append(item)
            item['grants']['response']=recovery.statement(pg,database,output,label+'-grant',grant)
            item['grants'].update(after=retirement.catalog(pg,database),privileges_after=privileges(pg,database))
            operator=None
            if suffix!='normal':
                mode=modes[0]
                sql=operator_sql(mode)
                operator=dict(statement=sql,before=rows(database),response=None)
                lab.write_json(output/(label+'-operator.json'),operator)
                try:
                    operator['response']=recovery.statement(pg,database,output,label+'-operator',sql)
                finally:
                    try:
                        operator['after']=rows(database)
                    except Exception as error:
                        operator['capture_error']=type(error).__name__
                    lab.write_json(output/(label+'-operator.json'),operator)
            shared=dict(seed=seed,clone=item)
            for mode in modes:
                legacy=None
                if mode=='legacy-grants':
                    revoke_new='\n'.join(s.replace('GRANT EXECUTE','REVOKE EXECUTE').replace(' TO ',' FROM ') for s in retirement.GRANTS)+'\n'
                    legacy=dict(statement=revoke_new,before=retirement.catalog(pg,database),response=None)
                    item['legacy_restore']=dict(statement='\n'.join(retirement.GRANTS)+'\n',response=None,catalog=None)
                    try:
                        legacy['response']=recovery.statement(pg,database,output,label+'-legacy-revoke',revoke_new)
                    finally:
                        try:
                            legacy['after']=retirement.catalog(pg,database)
                        except Exception as error:
                            legacy['capture_error']=type(error).__name__
                        lab.write_json(output/(label+'-legacy-revoke.json'),legacy)
                case=execute(pg,database,output,rows,directory,mode,statement_set(rows(database))[mode],shared,operator,legacy)
                context['cases'].append(case)
                lab.write_json(output/(MODE+'-verification.json'),context)
            observe_backend_absence(pg,database,output,label+'-after')
            answer,_=capture(client,output,label+'-after-dump','dump-state','/lab/'+label+'/store')
            assert answer.returncode==0 and not answer.stderr
            item['native_after']=json.loads(answer.stdout)
            lab.write_json(output/(MODE+'-verification.json'),context)
    finally:
        for item,revoke in owned:
            restored=dict(statement=revoke,response=None,catalog=None,privileges=None,errors=[])
            try:
                if item['legacy_restore'] is not None:
                    restore=item['legacy_restore']
                    restore['response']=recovery.statement(pg,item['database'],output,MODE+'-legacy-restore',restore['statement'])
                    restore['catalog']=retirement.catalog(pg,item['database'])
                restored['response']=recovery.statement(pg,item['database'],output,item['database']+'-restore',revoke)
                restored.update(catalog=retirement.catalog(pg,item['database']),privileges=privileges(pg,item['database']))
            except Exception as error:
                restored['errors'].append(type(error).__name__)
            item['restored']=restored
            lab.write_json(output/(item['database']+'-restored.json'),restored)
        if context is not None:
            lab.write_json(output/(MODE+'-verification.json'),context)
        if origin is not None:
            old,=json.loads(lab.docker('inspect',origin['client_id']).stdout)
            preserved=dict(control=rows(spec['database'])==seed['control'],files=census(store)==origin['files'],
                inputs=inputs_at(store)==seed['inputs'],contents=recovery.contents_at(store)==seed['contents'],
                catalog=retirement.catalog(pg,spec['database'])==seed['catalog'],
                client=old['State']==origin['client_state'] and old['Config']['Labels'][origin['label']]==spec['owner'],
                artifacts=all(digest(basis/n)==v for n,v in frozen.items()))
            lab.write_json(output/(MODE+'-source-preserved.json'),preserved)
            assert all(preserved.values()),preserved
    evidence,controls=verify(**context),negative_controls(**context)
    return dict(name='activated-retirement-sql-guards',status='PASS',sql_attempts=len(CASES),
        cases=[dict(name=c['mode'],status='PASS') for c in context['cases']],oracle=evidence,negative_controls=controls,
        source_preserved=preserved,not_run=['typed native retirement authority','ordinary source OPEN transition',
            'zero-request/import lifecycle','COMMIT loss or concurrent retirement','Archive restart and full M1'])
