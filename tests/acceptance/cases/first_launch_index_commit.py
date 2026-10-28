"""Actual same-store Index publication COMMIT loss and independently checked retry."""
from pathlib import Path
import json
import re
import shutil
import sys
import time
import tomllib
import xml.etree.ElementTree as ET

from cases.first_launch import digest, require, save
from cases.first_launch_index import DATABASE
from cases.first_launch_index_restart import RestartIndexRuntime, STAGES as RESTART_STAGES, LIMITS as RESTART_LIMITS
from cases.first_launch_index_pin import ShortPinRuntime, process_identity
from cases.first_launch_index_pin_release import custody
from cases import first_launch_index_oracle as index
from cases import first_launch_index_commit_oracle as oracle

CASES = {'p01-first-launch-index-commit-before':'before-forward','p01-first-launch-index-commit-after':'after-complete'}
STAGES = RESTART_STAGES[:6]+('index_publication_commit_loss',)+RESTART_STAGES[6:]
LIMITS = RESTART_LIMITS[:3]+[
    'The closed source and real pending plan3 feed a finite catch-up. Only exact consumer/revision3/topology0/previous2 UPDATE1 selects its actual publication COMMIT; no COMMIT ordinal is used.',
    'Both actual fault commands remain CommitUnknown/exit4 without a success record or claimed owner release. Independent raw PG state distinguishes unchanged revision2 from committed revision3, with no plan4.',
    'Fresh catch-up2→3 resolves the actual published/pending tuple; exact retry3→4 adds no work. Original plan3 origin1 remains; its publisher is3 before-forward or2 after-complete. New plan4 origin/publisher is3.',
    'Raw immutable replay is explicit: before-forward has two identical copies of the four plan3 dictionary rows, after-complete one. Exact retry must add no further copy; logical science and all40 receipts remain exact.',
    'The fresh root explicitly declares the unchanged runtime.renewal_timeout=5s deployment default. Original source/client/40s history/12s signed export bounds and proxy64MiB/20,000 frames/16 lifetime connections/120s remain. The observation hold must fit the existing SQL operation, or fail.',
    'Writer-only query lifecycle logging adds explicit server/background overhead within the original engine/disk limits. Its queue is1024 rows; after all application joins, collection is capped at8192 rows/4MiB/30s. Missing log events never establish query termination or exclusion.',
    'NOT_RUN: unknown ClickHouse query/remote exclusion, genuine unknown native termination, topology handoff and complete M1. These PG cases do not resolve or qualify those separate outcomes.',
]

TRACE_ROWS=8192
TRACE_BYTES=4*1024**2
TRACE_COLUMNS={'event','query_id','initial_query_id','user','initial_user','event_us','start_us',
               'duration_ms','exception_code','is_initial_query'}
TRACE_SETTINGS={'log_queries':'1','log_queries_min_type':"'QUERY_START'",'log_queries_min_query_duration_ms':'0',
                'log_queries_probability':'1','log_queries_cut_to_length':'1','log_query_threads':'0',
                'log_query_settings':'0','log_profile_events':'0'}


def query_trace_profile(server,users,provision):
    """Opt in only this closed writer; credential-bearing provisioner stays off."""
    require(ET.fromstring(users).findtext('./profiles/default/log_queries')=='0','default query logging must stay disabled')
    root=ET.fromstring(server)
    require(root.tag=='clickhouse' and root.find('query_log') is None,'fresh query lifecycle configuration')
    log=ET.SubElement(root,'query_log')
    for name,value in {'database':'system','table':'query_log','flush_interval_milliseconds':'7500',
                       'max_size_rows':'1024','reserved_size_rows':'64','buffer_size_rows_flush_threshold':'512',
                       'flush_on_crash':'false'}.items():ET.SubElement(log,name).text=value
    lines=provision.splitlines(keepends=True)
    selected=[n for n,line in enumerate(lines) if line.startswith('CREATE USER writer ')]
    require(len(selected)==1,'one stable Index writer')
    n=selected[0];marker=' SETTINGS max_concurrent_queries_for_user=1 MIN 1 MAX 1;'
    require(lines[n].count(marker)==1,'unchanged writer concurrency constraint')
    lines[n]=lines[n].replace(marker,marker[:-1]+', '+', '.join(k+'='+v for k,v in TRACE_SETTINGS.items())+';')
    return ET.tostring(root,encoding='unicode')+'\n',''.join(lines)


def lifecycle_summary(count,rows,positive):
    """Logged lifecycle pairs are observations, never a ProcessList removal proof."""
    require(type(count) is int and 0<=count<=TRACE_ROWS and len(rows)==count,'query lifecycle count/capture incomplete')
    groups={}
    for row in rows:
        require(set(row)==TRACE_COLUMNS and row['user']==row['initial_user']=='writer'
            and row['is_initial_query']==1,'query lifecycle owner/columns')
        query=row['query_id']
        require(query==positive or re.fullmatch(r'bigbrotr-index-[0-9a-f]{32}-[1-9][0-9]*-[1-9][0-9]*',query),
                'unexpected Index query identity')
        require(row['initial_query_id']==query and row['event'] in
            ('QueryStart','QueryFinish','ExceptionBeforeStart','ExceptionWhileProcessing'),'query lifecycle identity/event')
        require(all(type(row[k]) is int and row[k]>=0 for k in ('event_us','start_us','duration_ms','exception_code'))
            and row['event_us']>=row['start_us'],'query lifecycle time/code')
        groups.setdefault(query,[]).append(row)
    control=groups.get(positive,[])
    require(len(control)==2 and {r['event'] for r in control}=={'QueryStart','QueryFinish'}
        and all(r['exception_code']==0 for r in control),'actual named writer query lifecycle missing')
    missing=[]
    for query,events in groups.items():
        kinds=[r['event'] for r in events]
        paired=(len(kinds)==2 and kinds.count('QueryStart')==1 and
                sum(kinds.count(k) for k in ('QueryFinish','ExceptionWhileProcessing'))==1)
        before=kinds==['ExceptionBeforeStart']
        if not (paired or before):missing.append(query)
    return dict(status='INCOMPLETE' if missing else 'CAPTURED',rows=count,queries=len(groups),
        missing_or_duplicate_lifecycle=sorted(missing),exceptions=[dict(query_id=r['query_id'],event=r['event'],
            event_us=r['event_us'],exception_code=r['exception_code']) for r in rows if r['exception_code']],
        all_query_coverage='NOT_PROVEN',termination_or_exclusion='NOT_PROVEN',query_finish_is_process_list_removal=False)


def publication_operation_limit(config):
    """Closed deployment profile; restart settings do not belong to policy."""
    require(config.get('runtime',{}).get('renewal_timeout')=='5s',
            'Index publication requires the declared original 5s SQL operation cap')
    return 5_000_000


class IndexCommitRuntime(RestartIndexRuntime):
    bounded_sql = ShortPinRuntime.bounded_sql

    def prepare_state(self):
        super().prepare_state()
        # Declare the existing schema default before initialization, so the
        # observer can bind its cap to the actual deployed root without reading
        # a restart-only setting from the managed policy document.
        self.config.write_text(self.set_value(self.config.read_text(),'runtime','renewal_timeout','5s'))
        self.inputs['initial_recipe_sha256']=digest(self.config)
        private=self.state/'clickhouse/private'
        server,provision=query_trace_profile((private/'server.xml').read_text(),(private/'users.xml').read_text(),
                                            (private/'provision.sql').read_text())
        (private/'server.xml').write_text(server);(private/'provision.sql').write_text(provision)
        self.trace_output=self.root/'clickhouse-query-lifecycle';self.trace_output.mkdir(mode=0o700)
        self.trace_positive='bigbrotr-index-trace-'+self.owner
        save(self.trace_output/'profile.json',dict(writer_settings=TRACE_SETTINGS,queue_rows=1024,reserved_rows=64,
            flush_threshold_rows=512,flush_milliseconds=7500,capture_rows=TRACE_ROWS,capture_bytes=TRACE_BYTES,
            capture_seconds=30,query_text_limit_bytes=1,background_overhead=True,engine_limits='unchanged'))

    def bootstrap(self):
        super().bootstrap()
        observed=self.ch("SELECT name,value,min,max FROM system.settings WHERE name IN ('max_concurrent_queries_for_user',"
            "'log_queries','log_queries_cut_to_length','log_query_threads','log_query_settings','log_profile_events') ORDER BY name",
            'writer',rows=True)
        by_name={row['name']:row for row in observed}
        require(set(by_name)=={'max_concurrent_queries_for_user','log_queries','log_queries_cut_to_length',
            'log_query_threads','log_query_settings','log_profile_events'},'writer trace settings capture')
        require(by_name['max_concurrent_queries_for_user']==dict(name='max_concurrent_queries_for_user',value='1',min='1',max='1'),
            'writer concurrency constraint changed')
        for name,value in TRACE_SETTINGS.items():
            if name in by_name:require(by_name[name]['value']==value,'writer trace setting changed')
        save(self.trace_output/'writer-settings.json',observed)
        result=self.docker('exec','-i',self.ch_entry['id'],'clickhouse-client','--config-file','/run/ch-private/writer.xml',
            '--query_id',self.trace_positive,'--max_result_rows=1','--max_result_bytes=1024','--max_execution_time=5',
            '--receive_timeout=5',data='SELECT 1',timeout=10,label='index-trace-positive')
        require(result.stdout.strip()=='1' and not result.stderr,'positive actual writer query failed')
        self.collect_query_lifecycle('bootstrap')

    def collect_query_lifecycle(self,label):
        self.inspect(self.ch_entry)
        used=sum(p.stat().st_size for folder in ('data','logs') for p in (self.state/'clickhouse'/folder).rglob('*') if p.is_file())
        require(used<2*1024**3 and shutil.disk_usage(self.root).free>65*1024**3,'unchanged ClickHouse disk/completion allowance')
        self.resources['clickhouse_disk']=dict(bytes=used,free_bytes=shutil.disk_usage(self.root).free,limit_bytes=2*1024**3)
        self.record_resources()
        selected="user='writer' AND startsWith(query_id,'bigbrotr-index-')"
        sql="SYSTEM FLUSH LOGS; SELECT 'count' AS kind,count() AS rows FROM system.query_log WHERE "+selected+" FORMAT JSONEachRow; "
        sql+=("SELECT 'query' AS kind,toString(type) AS event,query_id,initial_query_id,user,initial_user,"
              "toUnixTimestamp64Micro(event_time_microseconds) AS event_us,"
              "toUnixTimestamp64Micro(query_start_time_microseconds) AS start_us,"
              "query_duration_ms AS duration_ms,exception_code,is_initial_query FROM system.query_log WHERE "+selected+
              f" ORDER BY event_time_microseconds,query_id,type LIMIT {TRACE_ROWS+1} FORMAT JSONEachRow")
        result=self.docker('exec','-i',self.ch_entry['id'],'clickhouse-client','--config-file','/run/ch-private/provisioner.xml',
            '--output_format_json_quote_64bit_integers=0',f'--max_result_rows={TRACE_ROWS+1}',f'--max_result_bytes={TRACE_BYTES}',
            '--result_overflow_mode=throw','--max_execution_time=10','--receive_timeout=15','--multiquery',
            data=sql,timeout=30,check=False,label='index-query-lifecycle-'+label)
        require(result.returncode==0 and not result.stderr and len(result.stdout.encode())<=TRACE_BYTES,'query lifecycle flush/export incomplete')
        values=[json.loads(line) for line in result.stdout.splitlines()]
        require(values and set(values[0])=={'kind','rows'} and values[0]['kind']=='count','query lifecycle independent count missing')
        rows=[]
        for value in values[1:]:
            require(value.pop('kind',None)=='query','query lifecycle export row')
            rows.append(value)
        save(self.trace_output/(label+'-rows.json'),rows)
        summary=lifecycle_summary(values[0]['rows'],rows,self.trace_positive)
        save(self.trace_output/(label+'-summary.json'),summary)

    def stop_container(self,entry,output=None):
        try:
            if entry['service']=='clickhouse' and hasattr(self,'trace_output') and not getattr(self,'trace_collected',False):
                self.trace_collected=True
                try:
                    require(not self.processes and all(e.get('status')=='stopped-retained' and e.get('pid')==0
                        for e in self.resources['containers'] if e['service'] not in ('clickhouse','database')),
                        'query lifecycle collection requires actual application/client joins')
                    self.collect_query_lifecycle('terminal')
                except BaseException as error:
                    self.resources['cleanup_errors'].append(dict(resource='query-lifecycle-capture',error=type(error).__name__))
                    save(self.trace_output/'terminal-summary.json',dict(status='INCOMPLETE',error_type=type(error).__name__,
                        termination_or_exclusion='NOT_PROVEN'))
        finally:
            # Even interruption or a failed diagnostic write cannot skip the
            # existing owned stop/join or replace the original stage failure.
            terminal=super().stop_container(entry,output)
        return terminal

    def candidate(self):
        for path in (Path(__file__),Path(oracle.__file__)):
            require(digest(path)==digest(self.source/'tests/acceptance/cases'/path.name),'Index COMMIT acceptance source mismatch')
        super().candidate()
        relative='experiments/archive-engine/native_commit_proxy.py'
        self.inputs['acceptance_files_sha256'][relative]=digest(self.source/relative)

    def load_helpers(self):
        super().load_helpers()
        sys.path.insert(0,str(self.source/'experiments/archive-engine'))
        from fault_cases import Process
        from signed_publication_cases import event, send, proxy_observation
        self.proxy_process,self.proxy_event,self.proxy_send,self.proxy_observation=Process,event,send,proxy_observation

    def index_commit_fault(self):
        self.source_unchanged()
        original=self.config.read_text()
        operation_limit=publication_operation_limit(tomllib.loads(original))
        self.commit_mode=CASES[self.args.case]
        output=self.root/'index-commit';output.mkdir(mode=0o700);self.commit_output=output
        consumer=index.only(self.closed['consumers'],consumer_role='index')['consumer_id'][2:]
        path=self.source/'experiments/archive-engine/native_commit_proxy.py'
        args=[sys.executable,'-B',str(path),'--upstream-port','15432','--mode',self.commit_mode,
            '--target','control-index-publication','--defer-arm','--index-consumer',consumer,'--hold-index-publication']
        proxy=None;failure=None
        evidence=dict(mode=self.commit_mode,operation_limit_us=operation_limit)
        try:
            proxy=self.proxy_process(args,input_pipe=True)
            save(output/'proxy-process.json',dict(argv=args,identity=process_identity(proxy.process.pid),source_sha256=digest(path)))
            ready=self.proxy_event(proxy,'ready',10);save(output/'proxy-ready.json',ready)
            require(ready['source_sha256']==digest(path) and ready['upstream_port']==15432 and ready['armed'] is False,
                    'owned deferred Index publication proxy')
            self.config.write_text(self.set_value(original,'postgres.control','endpoint',f"127.0.0.1:{ready['port']}"))
            self.proxy_send(proxy,'arm\n');self.proxy_event(proxy,'armed',1)
            deadline=time.monotonic()+120
            entry=self.launch('index-commit-fault','app','index','catch-up','--file','/etc/bigbrotr/instance.toml')
            parent=process_identity(self.inspect(entry)['State']['Pid']);require(parent is not None,'actual finite owner')
            evidence['parent']=parent
            evidence['selected']=self.proxy_event(proxy,'index-publication-update-complete',min(110,deadline-time.monotonic()))
            evidence['held']=self.proxy_event(proxy,'held',1)
            evidence['held_started_ns']=time.monotonic_ns()
            self.held_state,catalog=self.capture(output,'held')
            index.exact(catalog,self.restart_catalog)
            hold_deadline=min(deadline,evidence['held_started_ns']/1e9+operation_limit/1e6)
            selected_pid=evidence['selected']['backend_pid']
            require(type(selected_pid) is int and selected_pid>0,'selected actual backend PID')
            observed=self.bounded_sql('bigbrotr_control',f"SELECT json_build_object('now_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,'backend',(SELECT json_build_object('pid',pid,'backend_start',backend_start,'usename',usename,'datname',datname) FROM pg_stat_activity WHERE pid={selected_pid}))",hold_deadline)
            save(output/'held-backend.json',observed)
            oracle.held(self.closed,self.held_state,self.state/'data/index',observed['backend'],observed['now_us'])
            require(process_identity(parent['pid'])==parent,'same actual finite owner at publication')
            lock=self.state/'data/index/.bigbrotr-index-custody/custody.lock'
            evidence['custody_held']=custody(lock)
            require(evidence['custody_held']['busy'] is True,'actual Index custody absent at publication')
            evidence['allow_ns']=time.monotonic_ns()
            require(time.monotonic()<hold_deadline,'observer consumed original publication operation')
            save(output/'evidence.json',evidence)
            self.proxy_send(proxy,'release\n');self.proxy_event(proxy,'released',1)
            evidence['fault']=self.proxy_event(proxy,'fault',min(5,deadline-time.monotonic()))
            ended=self.docker('wait',entry['id'],timeout=min(15,deadline-time.monotonic()),label='index-commit-owner-join')
            info=self.inspect(entry);logs=self.docker('logs',entry['id'])
            (output/'serve.stdout').write_text(logs.stdout);(output/'serve.stderr').write_text(logs.stderr)
            save(output/'terminal-container.json',dict(id=info['Id'],state=info['State']))
            require(ended.stdout.strip()=='4' and info['State']['ExitCode']==4 and not info['State']['Running']
                    and info['State']['Pid']==0 and not info['State']['OOMKilled'],'actual failed publication exit4')
            require(not logs.stdout.strip() and len(logs.stderr)<=65536 and 'CommitUnknown' in logs.stderr,
                    'failed publication must retain uncertainty without terminal success')
            evidence.update(exit_code=4,commit_unknown=True,success_record=False,owner_release_claimed=False,
                            parent_after=process_identity(parent['pid']))
            self.faulted,_=self.capture(output,'faulted')
            self.preserved(output,'faulted')
            evidence['backend_absent']=True
            evidence['custody_joined']=custody(lock,evidence['custody_held']['identity'])
            rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
            save(output/'clickhouse-faulted.json',rows)
            generation=index.only(self.faulted['projection_generations'])['generation_id']
            index.physical_rows(rows,self.index_science,generation,self.schema,through=7)
            oracle.fault(self.held_state,self.faulted,evidence,self.state/'data/index')
            save(output/'fault-verdict.json',dict(status='PASS',mode=self.commit_mode,product='CommitUnknown',
                available=False,index_revision=2+int(self.commit_mode=='after-complete'),prepared_plan=3 if self.commit_mode=='before-forward' else None,
                new_plan4=False,remote_query_resolution='NOT_RUN'))
        except BaseException as error:
            failure=error;save(output/'original-failure.json',dict(error_type=type(error).__name__,reason=str(error)))
            raise
        finally:
            self.config.write_text(original)
            save(output/'evidence.json',evidence)
            if proxy is not None:
                cleanup_failure=None
                try:
                    self.proxy_send(proxy,'stop\n');require(proxy.wait(5)==0,'actual Index publication proxy join')
                    terminal=self.proxy_observation(proxy)
                    require(not terminal['stderr'] and not terminal['overflow'],'bounded proxy terminal output')
                except Exception as error:
                    self.resources['cleanup_errors'].append(dict(resource='index-publication-proxy',error=type(error).__name__))
                    cleanup_failure=error
                finally:
                    try:
                        proxy.stop()
                    except Exception as error:
                        self.resources['cleanup_errors'].append(dict(resource='index-publication-proxy',error='forced join '+type(error).__name__))
                        if cleanup_failure is None:cleanup_failure=error
                    finally:
                        save(output/'proxy-terminal.json',self.proxy_observation(proxy))
                if cleanup_failure is not None and failure is None:raise cleanup_failure
        self.source_unchanged()

    def index_catch_up(self):
        report=self.finite('index-commit-recover','index','catch-up','--file','/etc/bigbrotr/instance.toml')
        self.caught_up,_=self.capture(self.commit_output,'caught-up')
        oracle.report(report,self.caught_up,self.commit_mode)
        oracle.recovered(self.faulted,self.caught_up,self.state/'data/index',self.commit_mode)
        self.preserved(self.commit_output,'caught-up')
        self.index_rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        save(self.commit_output/'clickhouse-caught-up.json',self.index_rows)
        index.publication(self.caught_up,self.caught_up,self.index_rows,self.index_science,self.schema,
                          self.state/'data/index',owner_count=3,publication_fault=self.commit_mode)

    def index_retry(self):
        report=self.finite('index-commit-retry','index','catch-up','--file','/etc/bigbrotr/instance.toml')
        self.retried,_=self.capture(self.commit_output,'retry')
        oracle.report(report,self.retried,self.commit_mode,retry=True)
        oracle.recovered(self.caught_up,self.retried,self.state/'data/index',self.commit_mode,retry=True)
        self.preserved(self.commit_output,'retry')
        rows={name:self.ch(index.select_rows(self.schema,DATABASE,name),rows=True) for name in index.TARGETS}
        save(self.commit_output/'clickhouse-retry.json',rows);index.exact(rows,self.index_rows)

    def verify_index(self):
        result=index.publication(self.retried,self.retried,self.index_rows,self.index_science,self.schema,
            self.state/'data/index',owner_count=4,publication_fault=self.commit_mode)
        tables=self.ch(f"SELECT name,toString(uuid) AS uuid,engine_full FROM system.tables WHERE database='{DATABASE}' ORDER BY name",rows=True)
        columns=self.ch(f"SELECT table,name,type,position,default_kind,default_expression FROM system.columns WHERE database='{DATABASE}' ORDER BY table,position",rows=True)
        index.schema_metadata(self.schema,self.state/'data/index',tables,columns)
        save(self.commit_output/'clickhouse-schema.json',dict(tables=tables,columns=columns))
        index.exact(self.census(self.commit_output,'retry'),self.baseline_census)
        result.update(format='bigbrotr-installed-index-publication-commit/1',mode=self.commit_mode,
            live_observation=False,readiness='positive_prefix_before_pause',product_fault='CommitUnknown',
            origin_epochs=[1,1,1,3],publisher_epochs=[1,1,3 if self.commit_mode=='before-forward' else 2,3],
            owner_epochs=[1,2,3,4],control_tables=81,raw_family_counts={name:len(rows) for name,rows in self.index_rows.items()},
            remote_uncertainty_resolution='NOT_RUN',native_termination='NOT_RUN')
        save(self.commit_output/'verdict.json',result);self.inputs['index']=result
        self.source_unchanged()
