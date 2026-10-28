"""Actual same-store Index publication COMMIT loss and independently checked retry."""
from pathlib import Path
import sys
import time

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
    'Original source/client/40s history/12s signed export bounds and proxy64MiB/20,000 frames/16 lifetime connections/120s remain. The observation hold must fit the existing SQL operation, or fail.',
    'NOT_RUN: unknown ClickHouse query/remote exclusion, genuine unknown native termination, topology handoff and complete M1. These PG cases do not resolve or qualify those separate outcomes.',
]


class IndexCommitRuntime(RestartIndexRuntime):
    bounded_sql = ShortPinRuntime.bounded_sql

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
        self.commit_mode=CASES[self.args.case]
        output=self.root/'index-commit';output.mkdir(mode=0o700);self.commit_output=output
        consumer=index.only(self.closed['consumers'],consumer_role='index')['consumer_id'][2:]
        path=self.source/'experiments/archive-engine/native_commit_proxy.py'
        args=[sys.executable,'-B',str(path),'--upstream-port','15432','--mode',self.commit_mode,
            '--target','control-index-publication','--defer-arm','--index-consumer',consumer,'--hold-index-publication']
        original=self.config.read_text();proxy=None;failure=None
        evidence=dict(mode=self.commit_mode)
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
            operation_limit=index.only(self.held_state['policy_revisions'])['document']['settings']['runtime.renewal_timeout']
            require(type(operation_limit) is int and 0 < operation_limit <= 5_000_000,'original SQL operation cap')
            evidence['operation_limit_us']=operation_limit
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
