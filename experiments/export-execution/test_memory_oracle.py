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


if __name__ == '__main__':
    print(json.dumps(run(Path(sys.argv[1])), indent=2))
