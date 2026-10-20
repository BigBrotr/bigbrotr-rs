#!/usr/bin/env python3
"""Two actual proof-only COMMIT losses, each followed by config-only recovery/retry.

The existing transition owner excludes, copies and preserves both origins. The
proxy selects successful acquisition UPDATE occurrence two, never a COMMIT index.
An error remains an error; only the separate external predicates qualify it.
"""
from pathlib import Path
import shutil
import sys

sys.dont_write_bytecode = True
import transition
import installed_family as family
import installed_cycle
import installed_progress_commit_oracle as oracle
from installed import set_value, wait_for
from installed_family_capture import loads
from installed_progress_replay import PROGRESS_GRANTS
from fault_cases import Process

history, lab = transition.history, transition.lab
SUITE = transition.Suite(directory=oracle.DIRECTORY, label=oracle.LABEL, status=oracle.STATUS,
    client_prefix=oracle.CLIENT_PREFIX, database_prefix=oracle.PREFIX,
    clones=oracle.MODES, cases=oracle.CASES,
    scope='Two fresh activated/off copies: proof-only before-forward and after-complete COMMIT loss; '
        'joined failed Serve, authoritative readback, config-only recovery and exact retry',
    not_run=('ACK above zero, credits, GC, source/workflow completion or capacity qualification',
        'Mutation COMMIT loss, unjoined process recovery, and transport retry'))


class Fault:
    def __init__(self, pg, client, output, case):
        self.pg, self.client, self.output, self.case = pg, client, output, case
        self.proxy = self.current = None
        self.evidence = dict(format='bigbrotr-progress-commit-fault/1', mode=case['clone'],
            proxy_sha256=oracle.PROXY_SHA256, command=None, ready=None, gate=None,
            process=None, closed=False)

    def capture(self):
        with self.proxy.condition:
            self.evidence['process'] = dict(exit=self.proxy.process.poll(),
                stdout=bytes(self.proxy.stdout).decode(), stderr=bytes(self.proxy.stderr).decode(),
                overflow=self.proxy.overflow, readers_joined=all(not t.is_alive() for t in self.proxy.threads))
        lab.write_json(self.output/(self.case['name']+'-fault.json'), self.evidence)
        return self.evidence['process']

    def start(self):
        source = transition.SOURCE/'experiments/archive-engine/native_commit_proxy.py'
        assert history.digest(source) == oracle.PROXY_SHA256
        target = self.output/'progress-commit-proxy.py'
        if not target.exists(): shutil.copy2(source, target)
        assert history.digest(target) == oracle.PROXY_SHA256
        command = ['docker','exec','-i',self.client,'python3','-B','/lab/progress-commit-proxy.py',
            '--target','control-producer-progress','--mode',self.case['clone'],
            '--profile','retained-catch-up']
        self.evidence['command'] = command
        self.proxy = Process(command, input_pipe=True, limit=256*1024)
        def ready():
            value = self.capture()
            assert value['exit'] is None and not value['stderr'] and not value['overflow']
            lines = value['stdout'].splitlines(keepends=True)
            if not lines or not lines[0].endswith('\n'): return None
            event = loads(lines[0]); assert event['event'] == 'ready'
            assert event['source_sha256'] == oracle.PROXY_SHA256
            assert event['profile'] == 'retained-catch-up'
            assert event['frame_limit'] == 131072 and event['connection_limit'] == 64
            oracle.number(event['port'],1024,65535,integer=True)
            return event
        self.evidence['ready'] = wait_for(ready, 'progress fault proxy ready', 5)
        config = self.output/Path(self.case['config']).name
        original = config.read_text()
        (self.output/(self.case['clone']+'-direct-config.toml')).write_text(original)
        changed = set_value(original,'postgres.control','endpoint',
            '127.0.0.1:'+str(self.evidence['ready']['port']))
        config.write_text(changed)
        (self.output/(self.case['name']+'-config.toml')).write_text(changed)
        answer = lab.cli(dict(self.pg,client=self.client),'config','validate','--file',self.case['config'])
        lab.write_json(self.output/(self.case['name']+'-config-validation.json'),dict(
            returncode=answer.returncode,stdout=answer.stdout,stderr=answer.stderr))
        assert answer.returncode == 0 and answer.stderr == ''
        self.capture()

    def gate(self, captured, observed):
        self.current = max((r for r in observed['control']['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        # The SQL phase predicate has already required revision3/acquisition2,
        # C2 and eleven dependencies. Require the independent wire completion
        # before releasing the second actual REQ.
        assert len(observed['control']['producer_checkpoint_dependencies']) == 11
        for table in ('producer_requests','producer_outcomes'):
            assert len([r for r in observed['control'][table] if r['producer_epoch']==3]) == 2
        self.capture()
        event = oracle.wire_checked(self.evidence,self.current,gate_only=True)
        self.evidence['gate'] = dict(captured=captured,event=event)
        self.capture()

    def finish(self):
        if self.proxy is None: raise AssertionError('proxy was not started')
        try:
            self.proxy.process.stdin.write(b'stop\n'); self.proxy.process.stdin.flush()
            assert self.proxy.wait(5) == 0
            self.capture()
            self.evidence['closed'] = True
            oracle.wire_checked(self.evidence,self.current)
        except BaseException:
            self.evidence['closed'] = False
            raise
        finally:
            # This joins only the local wrapper. Unknown remote custody still
            # refuses native reopen and falls to the existing outer exclusion.
            self.proxy.stop()
            self.capture()

    def process_closed(self, response, case):
        oracle.process_closed(response,case)
        oracle.wire_checked(self.evidence,self.current)

    def verify(self, context): return oracle.verify(context)
    def controls(self, context): return oracle.controls(context)


def run_sequence(pg, client, output, clone, directory, expected, report):
    for name in (*family.HELPERS,'installed_cycle_client.py'):
        source = (transition.SOURCE/'experiments/ingest-bootstrap'/name if name=='installed_sync_client.py'
            else Path(__file__).with_name(name))
        target = output/name
        if not target.exists(): shutil.copy2(source,target)
        assert history.digest(target) == history.digest(source)
    family.configure(pg,client,output,clone['config'])
    mode = clone['name']; assert mode in oracle.MODES and clone['origin']=='activated'
    case = dict(name=mode+'-loss',clone=mode,origin='activated',operation='installed-live',
        predecessor=None,database=clone['database'],config=clone['config'],status='RUNNING')
    report['cases'].append(case); lab.write_json(output/'result.json',report)
    expected = family.execute(pg,client,output,case,directory,expected,clone['catalog_before'],
        progress=True,fault=Fault(pg,client,output,case))
    case['catalog_after'] = history.catalog(pg,clone['database'])
    assert case['catalog_after'] == clone['catalog_before']
    seed, predecessor = expected, case['name']
    # Only the operational endpoint changes back after positive fault custody.
    # Each invocation reads the same config path, whose exact phase text is saved.
    config = output/Path(clone['config']).name
    config.write_bytes((output/(mode+'-direct-config.toml')).read_bytes())
    for step in ('recover','retry'):
        case = dict(name=mode+'-'+step,clone=mode,origin='activated',operation='recover',
            predecessor=predecessor,database=clone['database'],config=clone['config'],status='RUNNING')
        report['cases'].append(case); lab.write_json(output/'result.json',report)
        expected = installed_cycle.execute(pg,client,output,case,directory,expected,
            clone['catalog_before'],seed,checks=(oracle.verify_recovery,
                lambda context,original:oracle.controls(context,original),oracle.recovery_closed))
        case['catalog_after'] = history.catalog(pg,clone['database'])
        assert case['catalog_after'] == clone['catalog_before']
        predecessor = case['name']; lab.write_json(output/'result.json',report)
    return predecessor


def run(root, build):
    # Same full tracked-source receipt used by the initial/activated progress
    # fixture. Capture before any engine action; never overwrite an earlier run.
    built = loads((build/'build-result.json').read_bytes())
    component = loads((build/'component-result.json').read_bytes())
    qualification = loads((build.parent/'qualification.json').read_bytes())
    oracle.qualification_checked(built,component,qualification)
    assert all(history.digest(build/name)==digest for name,digest in component['executables_sha256'].items())
    source = transition.SOURCE
    assert source == root/'source'
    assert not lab.command(['git','-C',str(source),'status','--porcelain','--untracked-files=all']).stdout
    revision = lab.command(['git','-C',str(source),'rev-parse','HEAD']).stdout.strip()
    files = lab.command(['git','-C',str(source),'ls-files']).stdout.splitlines()
    assert files and len(files)==len(set(files))
    path = root/'progress-source.json'; assert not path.exists() and not path.is_symlink()
    lab.write_json(path,dict(source=str(source),revision=revision,
        tree=lab.command(['git','-C',str(source),'rev-parse','HEAD^{tree}']).stdout.strip(),
        build=str(build),files_sha256={name:history.digest(source/name) for name in files}))
    return transition.run(root,build,suite=SUITE,sequence=run_sequence,
        origin_specs=(family.ORIGINS[0],),origin_for_clone={m:'activated' for m in oracle.MODES},
        extra_grants=(*installed_cycle.GRANTS,PROGRESS_GRANTS),producer_progress=True)


if __name__ == '__main__':
    assert len(sys.argv)==3
    run(Path(sys.argv[1]).resolve(strict=True),Path(sys.argv[2]).resolve(strict=True))
