#!/usr/bin/env python3
"""PostgreSQL resume boundary after the independently checked native seed."""
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import cases
from signed_publication_cases import command, sha


def main(root):
    state = json.loads((root / 'resources.json').read_text())
    source = root / 'source'
    result = dict(status='INCOMPLETE', source=state['source'], tree=state['tree'],
        scope='Unreceipted paused-to-queued refusal only; no API or output execution qualification')
    try:
        cases.main(root)
        trial = source / 'experiments/export-execution/resume_transition.sql'
        result['trial_sha256'] = sha(trial)
        output = command('docker', 'exec', '-i', state['postgres_id'], 'psql', '-XAtq',
            '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'bbpub_control_normal', input=trial.read_text())
        result['original_stdout'] = output
        observed = [json.loads(line) for line in output.splitlines() if line.startswith('{')]
        assert len(observed) == 1
        result['transition'] = observed[0]
        assert observed[0]['unchanged']
        assert observed[0]['code'] == '23514' and observed[0]['phase'] in ('update', 'constraints')
        assert observed[0]['message'] == 'paused export resume requires its atomic command receipt'
        result['status'] = 'PASS'
    except BaseException as error:
        result['status'] = 'FAIL'
        result['failure'] = type(error).__name__
        raise
    finally:
        (root / 'resume-schema-cases.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main(Path(sys.argv[1]).resolve())
