#!/usr/bin/env python3
"""Offline regression controls over a retained capture; never modify its files.

Matched selection mutations exercise the scope/manifest checks after the earlier
selection comparison. Artifact pins and the genuine baseline are checked by the
unmodified verification call first; these mutations are verifier controls, not
new runtime evidence.
"""
import copy
import json
from pathlib import Path
import sys
from unittest.mock import patch

import memory_oracle as oracle


def run(root):
    baseline = oracle.verify(root)
    initial = oracle.load(root / 'input.json')
    science_path = Path(initial['spec']['science'])
    science = oracle.load(science_path)
    original_scope = science['query']['scope']
    events = [json.loads(line) for line in (root / 'stdout').read_text().splitlines()]
    artifact = next(row['fields']['artifact'] for row in events if row.get('name') == 'reserved')
    manifest_path = Path(initial['case']['destination']) / artifact / 'manifest.json'
    original_manifest = manifest_path.read_bytes()
    original_load, read_bytes, read_text = oracle.load, Path.read_bytes, Path.read_text
    controls = []

    def check(name, *, scope=None, manifest=None, passes=False,
              reason='literal complete manifest bytes'):
        def load(path, maximum=16 * 1024 * 1024):
            value = original_load(path, maximum)
            if Path(path) == science_path and scope is not None:
                value = copy.deepcopy(value)
                value['query']['scope'] = scope
            return value

        def bytes_value(path):
            return manifest if path == manifest_path and manifest is not None else read_bytes(path)

        def text_value(path, *args, **kwargs):
            if path == root / 'stdout' and scope is not None:
                changed = copy.deepcopy(events)
                for row in changed:
                    if row.get('name') == 'reserved':
                        row['fields']['query']['scope'] = scope
                return ''.join(json.dumps(row) + '\n' for row in changed)
            return read_text(path, *args, **kwargs)

        with patch.object(oracle, 'load', load), patch.object(Path, 'read_bytes', bytes_value), \
                patch.object(Path, 'read_text', text_value):
            try:
                result = oracle.verify(root)
            except ValueError as error:
                if passes:
                    raise
                if str(error) != reason:
                    raise AssertionError('wrong rejection boundary: ' + name) from error
                controls.append(dict(name=name, status='REJECTED', reason=str(error)))
            else:
                if not passes or result != baseline:
                    raise AssertionError('incorrect oracle acceptance: ' + name)
                controls.append(dict(name=name, status='PASS_SAME_RESULT'))

    fields = ('dataset_id', 'dictionary_kind', 'allocation_domain', 'ordinal')
    for index in range(len(fields)):
        order = fields[index:] + fields[:index]
        check('scope-key-rotation-' + str(index),
              scope={key: original_scope[key] for key in order}, passes=True)
    check('scope-extra-key', scope=dict(original_scope, unrelated='forbidden'), reason='closed qualified scope')
    for key in fields:
        check('scope-missing-' + key, scope={k: v for k, v in original_scope.items() if k != key},
              reason='closed qualified scope')
    check('scope-wrong-value', scope=dict(original_scope, ordinal='999'))
    check('scope-wrong-type', scope=dict(original_scope, dictionary_kind='4'))
    manifest = json.loads(original_manifest)
    check('manifest-key-order', manifest=oracle.encoded(dict(reversed(list(manifest.items())))) + b'\n')
    check('manifest-extra-field', manifest=oracle.encoded(dict(manifest, unrelated=True)) + b'\n')
    check('manifest-duplicate-key', manifest=b'{"format":"bigbrotr-signed-events-export/1",' + original_manifest[1:])
    check('manifest-whitespace', manifest=b' ' + original_manifest)
    check('manifest-missing-newline', manifest=original_manifest[:-1])
    changed = copy.deepcopy(manifest)
    changed['selection']['scope']['ordinal'] = '999'
    check('manifest-wrong-scope-value', manifest=oracle.encoded(changed) + b'\n')
    return dict(status='PASS_OFFLINE_ORACLE_REGRESSION', baseline=baseline, controls=controls,
                scope='Read-only retained capture; virtual semantic/byte mutations, no engine or file changes')


def installed(payload):
    """Negative controls over an actual retained installed_five call payload."""
    # Resolve the same inert oracle dependencies as the setup owner. This import
    # performs no setup: all executable phases remain behind its main guard.
    import memory_setup
    from memory_setup_oracle import installed_five
    del memory_setup

    require_keys = {'before_capture', 'after_capture', 'selection', 'principal',
                    'commands', 'statuses', 'destination', 'science'}
    oracle.require(set(payload) == require_keys, 'closed installed oracle test payload')
    baseline = installed_five(**payload)
    controls = []
    new_jobs = {row['receipt']['job_id'] for row in payload['commands']}

    def row(changed, table, predicate=lambda value: True):
        before = changed['before_capture']['control'][table]
        return next(value for value in changed['after_capture']['control'][table]
                    if value not in before and predicate(value))

    def check(name, mutate):
        changed = copy.deepcopy(payload)
        mutate(changed)
        try:
            installed_five(**changed)
        except (ValueError, KeyError, StopIteration):
            controls.append(dict(name=name, status='REJECTED'))
        else:
            raise AssertionError('incorrect installed oracle acceptance: ' + name)

    check('missing-fifth-status', lambda value: value['statuses'].pop())
    check('changed-catalog', lambda value: value['after_capture']['catalog'].clear())
    check('missing-Control-table', lambda value: value['after_capture']['control'].pop('schema_versions'))
    check('duplicate-command-key', lambda value: value['commands'][1].update(key=value['commands'][0]['key']))
    check('historical-receipt', lambda value: value['commands'][0]['receipt'].update(created=False))
    check('false-terminal-status', lambda value: value['statuses'][0].update(state='running'))
    check('wrong-checkpoint', lambda value: row(value, 'jobs').update(checkpoint_revision=0))
    check('wrong-reservation-cap', lambda value: row(value, 'export_execution_units').update(max_output_bytes=1))
    check('wrong-artifact-digest', lambda value: row(value, 'artifacts').update(manifest_sha256='\\x' + 'aa' * 32))
    check('wrong-file-row-count', lambda value: row(value, 'artifact_files').update(row_count=0))
    check('missing-reader-release', lambda value: row(value, 'snapshot_leases',
        lambda item: item['state'] == 'released').update(state='active'))
    check('released-export-retention', lambda value: row(value, 'snapshot_leases',
        lambda item: item['state'] == 'active').update(state='released'))
    check('changed-retention-cut', lambda value: row(value, 'retention_claims').update(last_sequence=0))
    check('unrelated-audit-action', lambda value: row(value, 'management_audit').update(action='unrelated'))
    check('unreleased-Control', lambda value: row(value, 'owner_epochs').update(state='active'))
    check('rewritten-old-job', lambda value: next(item for item in value['after_capture']['control']['jobs']
        if item['job_id'][2:] not in new_jobs).update(xmin='999999999'))
    check('renewed-export-pin', lambda value: row(value, 'snapshot_leases',
        lambda item: item['state'] == 'active').update(lease_revision=2))
    return dict(status='PASS_OFFLINE_INSTALLED_ORACLE_REGRESSION', baseline=baseline, controls=controls,
                scope='Virtual mutations of caller-pinned retained captures only; no database, file or runtime changes')


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--installed':
        print(json.dumps(installed(oracle.load(sys.argv[2])), indent=2))
    else:
        print(json.dumps(run(Path(sys.argv[1])), indent=2))
