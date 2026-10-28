"""Installed actual reader-release COMMIT loss, following the original export journey."""
import fcntl
import http.client
import os
from pathlib import Path
import sys
import time

from cases.first_launch import Runtime, digest, require, save
from cases.first_launch_index import IndexRuntime, STAGES as INDEX_STAGES, LIMITS as INDEX_LIMITS, input_identity
from cases.first_launch_index_pin import ShortPinRuntime, READER_LEASES, process_identity, profile, reader_identity
from cases import first_launch_index_oracle as index
from cases import first_launch_index_pin_release_oracle as oracle
from cases import first_launch_restart

CASES = {'p01-first-launch-index-pin-release-before':'before-forward',
         'p01-first-launch-index-pin-release-after':'after-complete'}
STAGES = INDEX_STAGES[:-1] + ('pin_release_uncertainty','cleanup')
LIMITS = [item for item in INDEX_LIMITS if not item.startswith(('Ordinary Control must','Original ordinary journey'))] + [
    'The original signed history/export journey includes observed startup quiescence inside its40s/12s bounds, then committed owner release before a distinct ordinary Control lifetime tests release COMMIT loss.',
    'Fresh login waits only for actual existing bucket capacity (65s); setup has30s and the focused fault phase30s. Original3s lease,500ms renewal,5s authorization and30s full-probe bounds remain.',
    'One newly admitted reader pin and actual native worker identify the release; an exact eligible UPDATE1 hold requires positive native join and retained custody before permitting the actual COMMIT fault.',
    'The second lifetime gates its first full probe: exactly one active reader and authenticated building/verification_in_flight capabilities with an empty operation cache. Login and observation must fit the original live pin; no Ready is claimed.',
    'Before-forward requires unchanged active lease/claims; after-complete requires atomic released revision2 and matching changed xmin. Both product results remain CommitUnknown/exit4/unavailable with no claimed owner release or recovery.',
    'NOT_RUN: genuine native TerminationUnknown, automatic pin recovery, withheld inputs, dependency outages, and complete M1.',
]


def custody(path, expected=None):
    identity = input_identity(path,empty=True)
    if expected is not None:
        index.exact(identity,expected)
    fd = os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    try:
        stat = os.fstat(fd)
        require((stat.st_dev,stat.st_ino) == (identity['device'],identity['inode']), 'custody inode changed on open')
        try:
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            busy = True
        else:
            busy = False
            fcntl.flock(fd,fcntl.LOCK_UN)
    finally:
        os.close(fd)
    index.exact(input_identity(path,empty=True),identity)
    return dict(identity=identity,busy=busy)


class PinReleaseRuntime(ShortPinRuntime):
    # The original independent export oracle keeps its actual released owner.
    ordinary = Runtime.ordinary

    ordinary_client = IndexRuntime.baseline_client

    def candidate(self):
        for path in (Path(__file__),Path(oracle.__file__)):
            require(digest(path) == digest(self.source/'tests/acceptance/cases'/path.name),
                    'pin-release fixture source mismatch')
        super().candidate()

    def load_helpers(self):
        super().load_helpers()
        from installed_archive_profile import load as load_profile
        from installed_live_client import access
        self.login_profile, self.access = load_profile,access
        self.restart_checks = first_launch_restart

    def pin_versions(self, lease_id, deadline):
        require(lease_id.startswith('\\x') and len(lease_id) == 34
                and all(c in '0123456789abcdef' for c in lease_id[2:]), 'raw pin identity')
        selected = "lease_id=decode('"+lease_id[2:]+"','hex')"
        return self.bounded_sql('bigbrotr_control',f"""SELECT json_build_object(
            'lease',(SELECT row_to_json(l) FROM control.snapshot_leases l WHERE {selected}),
            'lease_xmin',(SELECT xmin::text FROM control.snapshot_leases WHERE {selected}),
            'pin',(SELECT row_to_json(p) FROM control.archive_snapshot_pins p WHERE {selected}),
            'pin_xmin',(SELECT xmin::text FROM control.archive_snapshot_pins WHERE {selected}),
            'claims',(SELECT json_agg(c ORDER BY claim_id) FROM control.retention_claims c WHERE {selected}),
            'claim_xmins',(SELECT json_agg(xmin::text ORDER BY claim_id) FROM control.retention_claims WHERE {selected}));""",deadline)

    def start_proxy(self, output, name, arguments):
        path = self.source/'experiments/archive-engine/native_commit_proxy.py'
        args = [sys.executable,'-B',str(path),'--upstream-port','15432','--defer-arm',
                '--catalog-manager','bigbrotr_catalog','--catalog-reader','bigbrotr_archive_read',
                '--catalog-database','bigbrotr_catalog',*arguments]
        proxy = self.proxy_process(args,input_pipe=True)
        # Register before the first fallible readiness observation.
        self.release_proxies.append((name,proxy))
        identity = process_identity(proxy.process.pid)
        save(output/(name+'-process.json'),dict(argv=args,identity=identity,source_sha256=digest(path)))
        ready = self.proxy_event(proxy,'ready',10)
        save(output/(name+'-ready.json'),ready)
        require(identity is not None and process_identity(proxy.process.pid) == identity
                and ready['upstream_host'] == '127.0.0.1' and ready['upstream_port'] == 15432
                and ready['source_sha256'] == digest(path) and ready['armed'] is False, 'owned deferred proxy route')
        return proxy,ready

    def pin_release_uncertainty(self):
        output = self.root/'pin-release'; output.mkdir(mode=0o700)
        mode = CASES[self.args.case]
        self.source_unchanged()
        self.wait_login_capacity(output)
        historical,catalog = self.capture(output,'historical')
        preserved = {name:self.files(self.state/'data'/name) for name in
                     ('archive','archive-state','export','scratch','ingest/producer-inputs-v1')}
        save(output/'before-files.json',preserved)
        journey = {name:digest(self.home/name) for name in ('export-observations.json','export-client-result.json')}
        self.release_proxies = []
        failure = None
        try:
            reader,ready = self.start_proxy(output,'reader',['--mode','hold-reader-query',
                '--profile','ordinary-reader','--lifetime-seconds','120'])
            require(ready['profile'] == 'ordinary-reader' and ready['connection_limit_scope'] == 'active'
                    and ready['frame_limit'] == 20000 and ready['connection_limit'] == 16, 'original reader proxy bounds')
            control,control_ready = self.start_proxy(output,'control',['--mode',mode,
                '--target','control-pin-release','--hold-pin-release','--lifetime-seconds','120'])
            require(control_ready['hold_pin_release'] is True and control_ready['target'] == 'control-pin-release'
                    and control_ready['mode'] == mode, 'exact held release proxy')
            changed = profile(self.config.read_text(),self.set_value,ready['port'])
            self.config.write_text(self.set_value(changed,'postgres.control','endpoint',f"127.0.0.1:{control_ready['port']}"))
            setup_started = time.monotonic_ns(); deadline = time.monotonic()+30
            clock_before = self.bounded_sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint',deadline)
            arm = self.bounded_sql('bigbrotr_control',
                "SELECT json_build_object('now_us',(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint,"
                "'active_readers',(SELECT count(*) "+READER_LEASES+"))",deadline)
            save(output/'before-arm.json',arm)
            require(arm['active_readers'] == 0, 'no earlier active reader pin')
            # Select the first native probe of this new lifetime. Parsing every
            # Control frame remains bounded by the unchanged20,000-frame limit.
            self.proxy_send(reader,'arm\n'); self.proxy_event(reader,'armed',1)
            entry = self.launch('pin-release','app','serve','--file','/etc/bigbrotr/instance.toml','--roles','control')
            first = self.wait(lambda:self.logs(entry,output),'focused ordinary Control',min(20,deadline-time.monotonic()),entry=entry)[0]
            require(first['state'] == 'idle', 'focused ordinary startup')
            parent = process_identity(self.inspect(entry)['State']['Pid'])
            require(parent is not None, 'actual focused Control parent')
            # The original session is revoked by its successful journey. Preserve
            # those protected bytes; the real login may prune only that row.
            saved_session = self.home/'export-session.private'
            saved_session.rename(output/'original-session.private')
            login = self.login_profile(self.home)
            self.access('login','--origin','http://127.0.0.1:18081','--username','alice',
                '--password-ref',login['password_ref'],'--session-output',str(saved_session),
                binary=login['product_binary'],failure_directory=output)
            before = self.exercise_release(output,mode,entry,parent,historical,catalog,reader,control,
                arm['now_us'],clock_before,setup_started,deadline)
            after,after_catalog = self.capture(output,'after')
            require(after_catalog == catalog, 'release fault changed scientific catalog')
            require(preserved == {name:self.files(self.state/'data'/name) for name in preserved}, 'release fault changed retained scientific/input/export files')
            require(journey == {name:digest(self.home/name) for name in journey}, 'user operation inside focused fault barrier')
            self.release_evidence['finished_ns'] = time.monotonic_ns()
            save(output/'release-evidence.json',self.release_evidence)
            verdict = oracle.verify(self.release_evidence,before,after)
            self.source_unchanged()
            save(output/'verdict.json',verdict)
            self.inputs['pin_release_uncertainty'] = verdict
        except BaseException as error:
            failure = error
            if hasattr(self,'release_evidence'):
                save(output/'release-evidence.json',self.release_evidence)
            save(output/'original-failure.json',dict(error_type=type(error).__name__,reason=str(error)))
            raise
        finally:
            errors = []
            for name,proxy in reversed(self.release_proxies):
                try:
                    self.proxy_send(proxy,'stop\n')
                    require(proxy.wait(5) == 0, 'proxy actual stop/join')
                    observed = self.proxy_observation(proxy)
                    require(not observed['stderr'] and not observed['overflow'], 'bounded proxy terminal output')
                except Exception as error:
                    errors.append(dict(resource='pin-release-'+name,error=type(error).__name__))
                finally:
                    try:
                        proxy.stop()
                    except Exception as error:
                        errors.append(dict(resource='pin-release-'+name,error='forced join '+type(error).__name__))
                    finally:
                        save(output/(name+'-terminal.json'),self.proxy_observation(proxy))
            self.resources['cleanup_errors'].extend(errors)
            if errors and failure is None:
                raise ValueError('release proxy cleanup incomplete')

    def exercise_release(self, output, mode, entry, parent, historical, catalog, reader, control,
                         arm_clock, clock_before, setup_started, setup_deadline):
        started = time.monotonic_ns(); fault_deadline = time.monotonic()+30
        deadline = min(setup_deadline,fault_deadline)
        evidence = dict(mode=mode,started_ns=started,parent=parent,arm_clock_us=arm_clock,
                        setup_started_ns=setup_started)
        self.release_evidence = evidence
        def retain():
            save(output/'release-evidence.json',evidence)
        retain()
        selected = self.proxy_event(reader,'reader-selected',min(17,deadline-time.monotonic()))
        gate_event = self.proxy_event(reader,'held',1)
        save(output/'reader-gate.json',dict(selected=selected,held=gate_event))
        require(gate_event['stage'] == 'reader-query' and selected['admitted_after_arm'] is True
                and selected['backend_pid'] == gate_event['backend_pid']
                and selected['connection_number'] == gate_event['target_connection']
                and selected['framed_connections'] == gate_event['framed_connections'] == 1, 'actual post-arm reader gate')
        worker = self.worker(parent); evidence['worker'] = worker
        before,before_catalog = self.capture(output,'before')
        value = self.roles(deadline); save(output/'setup-roles.json',value)
        clock_after = self.bounded_sql('bigbrotr_control','SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint',deadline)
        require(time.monotonic() < deadline and catalog == before_catalog, 'bounded unchanged setup catalog')
        baseline_owner = oracle.setup(historical,before,'\\x'+self.enrolled['principal_id'],clock_before,clock_after)
        oracle.setup_roles(value,before,clock_before,clock_after)
        evidence['baseline_owner'] = baseline_owner
        pins = self.bounded_sql('bigbrotr_control',"SELECT coalesce(json_agg(l),'[]') "+READER_LEASES,deadline)
        require(len(pins) == 1 and pins[0]['lease_id'] not in {row['lease_id'] for row in historical['snapshot_leases']}, 'unique new reader pin')
        lease_id = pins[0]['lease_id']
        gate = self.sample(lease_id,gate_event['backend_pid'],parent,worker,deadline)
        evidence['gate'] = gate
        evidence['versions'] = dict(before=self.pin_versions(lease_id,deadline)); retain()
        oracle.selected_pin(gate,baseline_owner)
        oracle.admission_versions(gate,evidence['versions']['before'],before)
        reader_identity(gate['reader'],gate['lease']['acquired_at_us'])
        require(arm_clock <= gate['lease']['acquired_at_us']
                and gate['reader']['backend_start_us'] <= gate['now_us'], 'actual first reader admission identity')
        index.exact(gate['parent'],parent); index.exact(gate['worker'],worker)
        index.exact(gate['control_backend'],dict(pid=baseline_owner['control_backend_pid'],backend_start=baseline_owner['control_backend_start']))
        backend = self.bounded_sql('bigbrotr_control',f"SELECT json_build_object('pid',pid,'backend_start',backend_start,'usename',usename,'datname',datname) FROM pg_stat_activity WHERE pid={baseline_owner['control_backend_pid']}",deadline)
        evidence['owner_backend'] = backend
        index.exact(backend,dict(pid=baseline_owner['control_backend_pid'],backend_start=baseline_owner['control_backend_start'],
            usename='bigbrotr_control',datname='bigbrotr_control'))
        path = self.state/'data/archive/.bigbrotr-initialization/custody.lock'
        evidence['custody'] = custody(path); retain()
        require(evidence['custody']['busy'] is True, 'native capture has no actual custody')
        # Arm only after one selected pin, one actual worker and the actual
        # original Control backend are independently bound. No user work follows.
        self.proxy_send(control,'arm\n'); self.proxy_event(control,'armed',1)
        evidence['release_gate_clock_us'] = self.bounded_sql('bigbrotr_control',
            'SELECT (EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint',deadline)
        require(gate['now_us'] <= evidence['release_gate_clock_us'] < gate['lease']['expires_at_us']
                and time.monotonic() < deadline, 'setup consumed original live pin or phase window')
        evidence['setup_finished_ns'] = time.monotonic_ns(); retain()
        save(output/'setup-verdict.json',dict(status='PASS',probe='first_in_flight',operation_cache='empty',
            capabilities='building/verification_in_flight',lease_id=lease_id,parent=parent,worker=worker,
            clock_before_us=clock_before,clock_after_us=clock_after))
        self.proxy_send(reader,'release\n'); self.proxy_event(reader,'released',1)
        deadline = fault_deadline
        evidence['update'] = self.proxy_event(control,'pin-release-update-complete',min(5,deadline-time.monotonic()))
        held_event = self.proxy_event(control,'held',min(1,deadline-time.monotonic()))
        evidence['hold_event'] = held_event
        held_started = time.monotonic_ns()
        require(held_event['stage'] == 'pin-release-update' and held_event['backend_pid'] == baseline_owner['control_backend_pid'], 'actual owner release UPDATE1 hold')
        # Local observations only in the existing <=500ms SQL operation. Slow
        # observers may fail; they cannot widen or replace that operation.
        held = dict(started_ns=held_started,parent=process_identity(parent['pid']),
            worker=self.worker(parent,worker),custody=custody(path,evidence['custody']['identity']),
            control_backend_pid=held_event['backend_pid'])
        held['finished_ns'] = time.monotonic_ns()
        evidence['held'] = held
        require(held['worker'] is None and held['custody']['busy'] is True, 'release attempted before actual native join or without retained custody')
        evidence['allow_ns'] = time.monotonic_ns()
        require(evidence['allow_ns']-held_started < 500_000_000, 'observer consumed original release operation window')
        self.proxy_send(control,'release\n')
        self.proxy_event(control,'released',1)
        retain()
        evidence['fault'] = self.proxy_event(control,'fault',min(2,deadline-time.monotonic())); retain()
        ended = self.docker('wait',entry['id'],timeout=min(8,deadline-time.monotonic()),label='release-owner-join')
        info = self.inspect(entry); stdout = self.logs(entry,output)
        save(output/'terminal-container.json',dict(id=info['Id'],state=info['State']))
        require(ended.stdout.strip() == '4' and info['State']['ExitCode'] == 4
                and not info['State']['Running'] and info['State']['Pid'] == 0 and not info['State']['OOMKilled'], 'actual CommitUnknown exit4')
        stderr = (output/'serve.stderr').read_text()
        require(len(stderr) <= 65536 and 'CommitUnknown' in stderr and 'pin outcome requires recovery' in stderr, 'release uncertainty must remain explicit')
        require(all(row.get('release') != 'committed' for row in stdout), 'failed owner falsely claimed committed release')
        require(process_identity(parent['pid']) is None and process_identity(worker['pid']) is None, 'actual parent/worker join incomplete')
        counts = []
        while True:
            require(len(counts) < 30 and time.monotonic() < deadline, 'remote backend drain bound')
            count = self.bounded_sql('bigbrotr_control',"SELECT count(*) FROM pg_stat_activity WHERE datname IN ('bigbrotr_control','bigbrotr_catalog') AND pid<>pg_backend_pid()",deadline)
            counts.append(count); save(output/'backend-drain.json',counts)
            if count == 0: break
            time.sleep(.05)
        connection = http.client.HTTPConnection('127.0.0.1',18081,timeout=min(1,deadline-time.monotonic()))
        try:
            try:
                connection.request('GET','/management/v1/roles')
            except ConnectionRefusedError:
                unavailable = 'connection-refused'
            else:
                raise ValueError('management listener remains after failed Control join')
        finally:
            connection.close()
        evidence['terminal'] = dict(exit_code=4,parent=process_identity(parent['pid']),worker=process_identity(worker['pid']),
            backend_count=count,http=unavailable,commit_unknown=True,owner_release_claimed=False)
        evidence['terminal_custody'] = custody(path,evidence['custody']['identity'])
        evidence['versions']['after'] = self.pin_versions(lease_id,deadline)
        evidence['terminal_finished_ns'] = time.monotonic_ns(); retain()
        require(time.monotonic() < deadline, 'original focused phase bound')
        return before
