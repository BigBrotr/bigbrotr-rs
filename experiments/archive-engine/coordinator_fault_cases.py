#!/usr/bin/env python3
"""Actual Control binding COMMIT ambiguity in one inventoried native lab.

No provisioning credentials or SQL bytes are recorded. Process-wrapper exit alone
is not remote exclusion: terminal lab audit must reconcile actual owned peers.
"""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from signed_publication_cases import (Process, command, event, execute, proxy_observation,
                                      send, sha)


def run(root):
    source = Path(__file__).resolve().parents[2]
    resources = json.loads((root/'resources.json').read_text())
    assert source == root/'source'
    assert command('git', '-C', str(source), 'rev-parse', 'HEAD') == resources['source']
    assert not command('git', '-C', str(source), 'status', '--porcelain', '--untracked-files=all')
    for key in ['container_id', 'postgres_id']:
        container = json.loads(command('docker', 'inspect', resources[key]))[0]
        assert container['State']['Running'] and not container['State']['OOMKilled']
        assert container['Config']['Labels']['org.bigbrotr.publication'] == resources['owner']
    ready = json.loads((root/'out/native-tests/ready.json').read_text())
    assert ready['source'] == resources['source']
    assert sha(root/'out/native-tests/archive-publication-probe') == ready['publisher']
    report = dict(status='INCOMPLETE', source=resources['source'], tree=resources['tree'],
                  artifacts=ready, harness_sha256=sha(Path(__file__)),
                  proxy_sha256=sha(source/'experiments/archive-engine/native_commit_proxy.py'), cases=[])
    path = root/'coordinator-fault-cases.json'
    assert not path.exists()

    def save():
        path.write_text(json.dumps(report, indent=2)+'\n')

    runner = resources['container_id']
    save()
    for mode in ['before', 'after']:
        case = dict(mode=mode, status='INCOMPLETE')
        report['cases'].append(case)
        save()
        proxy = None
        try:
            proxy = Process(['docker', 'exec', '-i', runner, 'python3', '-B',
                             '/source/experiments/archive-engine/native_commit_proxy.py',
                             '--target', 'control-coordinator', '--mode',
                             'before-forward' if mode == 'before' else 'after-complete'],
                            input_pipe=True)
            started = event(proxy, 'ready')
            assert started['source_sha256'] == report['proxy_sha256']
            client = case['client'] = {}
            execute(['docker', 'exec', '--env', 'LD_LIBRARY_PATH=/out/archive-worker/lib', runner,
                     '/out/native-tests/archive-publication-probe', 'protocol', mode,
                     str(started['port'])], 100, client)
            assert client['exit'] == 0 and not client['stderr'] and not client['overflow']
            send(proxy, 'stop\n')
            assert proxy.wait(5) == 0 and not proxy.stderr and not proxy.overflow
            terminal = event(proxy, 'terminal')
            assert terminal['unexpected'] == 0 and terminal['connections'] == 1
            assert terminal['data_queries'] == terminal['coordinator_inserts'] == terminal['data_commits'] == terminal['faults'] == 1
            assert terminal['client_complete'] == 0
            assert terminal['server_complete'] == int(mode == 'after')
            case['proxy_terminal'] = terminal
            values = [json.loads(line) for line in client['stdout'].splitlines()]
            assert len(values) == 1 and values[0]['event'] == 'protocol-case' and values[0]['name'] == mode
            state = case['state'] = values[0]['state']
            before, after = state['before_retry'], state['after_retry']
            assert len(before['bindings']) == int(mode == 'after') and len(after['bindings']) == 1
            assert not before['attempts'] and not after['attempts']
            digest = '\\x'+sha(root/f'out/coordinator-protocol-{mode}/state/coordinator.json')
            assert after['bindings'][0]['binding_sha256'] == digest
            assert after['bindings'][0]['issuer_epoch'] == (1 if mode == 'after' else 2)
            if mode == 'after':
                assert before == after
            case['status'] = 'PASS'
        except BaseException as error:
            case['failure'] = type(error).__name__
            case['status'] = report['status'] = 'FAIL' if isinstance(error, AssertionError) else 'INCOMPLETE'
            raise
        finally:
            if proxy:
                proxy.stop()
                case['proxy_observation'] = proxy_observation(proxy)
            save()
    report['status'] = 'PASS'
    save()
    print(json.dumps(dict(status='PASS', cases=len(report['cases']), source=report['source'])), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    run(parser.parse_args().root.resolve())
