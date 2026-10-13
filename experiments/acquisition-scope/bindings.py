#!/usr/bin/env python3
"""Scope-bound actual admission/certification; new PG clones and native stores only."""
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / 'experiments/control-migrations'))
import lab
sys.path.insert(0, str(SOURCE / 'experiments/relay-source'))
spec = importlib.util.spec_from_file_location('bootstrap_oracle', SOURCE / 'experiments/ingest-bootstrap/oracle.py')
oracle = importlib.util.module_from_spec(spec)
sys.modules['bootstrap_oracle'] = oracle
spec.loader.exec_module(oracle)
from oracle import complete_source
sys.path.insert(0, str(SOURCE / 'experiments/network-transport'))
from inventory import projection

root = Path(sys.argv[1]).resolve(strict=True)
assert len(sys.argv) in (2, 3, 4) and SOURCE == root / 'source'
guards = len(sys.argv) == 4 and sys.argv[2] == '--guards'
refusals = len(sys.argv) == 3 and sys.argv[2] == '--refusals'
policy = len(sys.argv) == 3 and sys.argv[2] == '--policy'
lease = len(sys.argv) == 3 and sys.argv[2] == '--lease'
session_hash = len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-sessions-hash'
session_faults = len(sys.argv) == 4 and sys.argv[2] == '--installed-sessions-faults'
source_http_regression = len(sys.argv) == 4 and sys.argv[2] == '--source-http-regression'
job_http_faults = len(sys.argv) == 4 and sys.argv[2] == '--job-http-faults'
scope_http_faults = len(sys.argv) == 4 and sys.argv[2] == '--scope-http-faults'
registration_http_signal = len(sys.argv) == 4 and sys.argv[2] == '--registration-http-signal'
registration_http_faults = source_http_regression or job_http_faults or scope_http_faults or registration_http_signal or (len(sys.argv) == 4 and sys.argv[2] == '--registration-http-faults')
scope_command_relations = len(sys.argv) == 4 and sys.argv[2] == '--scope-command-relations'
registration_relations = scope_command_relations or (len(sys.argv) == 4 and sys.argv[2] == '--registration-relations')
registration_races = len(sys.argv) == 4 and sys.argv[2] == '--registration-races'
scope_command_races = len(sys.argv) == 4 and sys.argv[2] == '--scope-command-races'
scope_command_faults = scope_command_relations or scope_command_races or (len(sys.argv) == 4 and sys.argv[2] == '--scope-command-faults')
registration_faults = scope_command_faults or registration_http_faults or registration_relations or registration_races or (len(sys.argv) == 4 and sys.argv[2] == '--registration-faults')
job_actors = len(sys.argv) == 4 and sys.argv[2] == '--installed-job-actors'
job_expiry = len(sys.argv) == 4 and sys.argv[2] == '--installed-job-expiry'
job_gap = len(sys.argv) == 4 and sys.argv[2] == '--installed-job-gap'
export_status = len(sys.argv) == 4 and sys.argv[2] == '--installed-export-status'
jobs_http = export_status or job_actors or job_expiry or job_gap or (len(sys.argv) == 4 and sys.argv[2] == '--installed-jobs')
scope_http = len(sys.argv) == 4 and sys.argv[2] == '--installed-scopes'
registration_http = len(sys.argv) == 4 and sys.argv[2] == '--installed-registrations'
enrollment_functions = len(sys.argv) == 4 and sys.argv[2] == '--installed-enrollment-functions'
enrollment_http = enrollment_functions or (len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-enrollments')
sessions = session_hash or session_faults or (len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-sessions')
installed_sync_visibility = len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-sync-visibility'
installed_sync_expiry = len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-sync-expiry'
installed_sync_window = len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-sync-window'
installed_sync = installed_sync_visibility or installed_sync_expiry or installed_sync_window or (len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-sync')
installed_archive_setup = len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-archive-setup'
installed_archive_signed = len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-archive-signed'
installed_archive_export = len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-archive-export'
installed_archive_publication = installed_archive_export or installed_archive_signed or (len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-archive')
installed_archive = installed_archive_setup or installed_archive_publication
ingest_recovery_interruptions = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-recovery-interruptions'
ingest_activated_acquisition_limits = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-acquisition-limits'
ingest_activated_acquisition_http = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-acquisition-http'
ingest_activated_acquisition_guards = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-acquisition-guards'
ingest_activated_acquisition_claims = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-acquisition-claims'
ingest_activated_acquisition_faults = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-acquisition-faults'
ingest_activated_acquisition = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-acquisition'
ingest_activated_source_prefix = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-source-prefix'
ingest_activated_source_locks = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-source-locks'
ingest_activated_source_durable = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-source-durable'
ingest_activated_source_faults = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-source-faults'
ingest_activated_source = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-source'
ingest_activated_refusals = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-refusals'
ingest_activated_zero = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-zero'
ingest_activated_faults = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-faults'
ingest_activated_recovery = len(sys.argv) == 4 and sys.argv[2] == '--ingest-activated-recovery'
ingest_installed_live = len(sys.argv) == 4 and sys.argv[2] == '--ingest-installed-live'
ingest_live_refusals = len(sys.argv) == 4 and sys.argv[2] == '--ingest-live-refusals'
ingest_live_routes = len(sys.argv) == 4 and sys.argv[2] == '--ingest-live-routes'
ingest_live_guards = len(sys.argv) == 4 and sys.argv[2] == '--ingest-live-guards'
ingest_live = len(sys.argv) == 4 and sys.argv[2] == '--ingest-live'
installed_empty_retirement = len(sys.argv) == 4 and sys.argv[2] == '--installed-empty-retirement'
ingest_empty_preparer = len(sys.argv) == 4 and sys.argv[2] == '--ingest-empty-preparer'
ingest_empty_locks = len(sys.argv) == 4 and sys.argv[2] == '--ingest-empty-locks'
ingest_empty_commits = len(sys.argv) == 4 and sys.argv[2] == '--ingest-empty-commits'
ingest_empty_retirement = ingest_empty_commits or len(sys.argv) == 4 and sys.argv[2] == '--ingest-empty-retirement'
ingest_gap_faults = len(sys.argv) == 4 and sys.argv[2] == '--ingest-gap-faults'
ingest_successor_gap = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-successor-gap'
ingest_successor_history = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-successor-history'
ingest_succession_commits = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-succession-commits'
ingest_succession = ingest_succession_commits or len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-succession'
ingest_retirement_faults = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-retirement-faults'
ingest_retirement_locks = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-retirement-locks'
ingest_retirement_obligations = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-retirement-obligations'
ingest_retirement = ingest_retirement_obligations or ingest_retirement_locks or ingest_retirement_faults or len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-retirement'
ingest_abandonment_faults = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-abandonment-faults'
ingest_abandonment_limits = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-abandonment-limits'
ingest_abandonment_guards = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-abandonment-guards'
ingest_abandonment = ingest_abandonment_faults or ingest_abandonment_limits or ingest_abandonment_guards or len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-abandonment'
ingest_source_faults = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-source-faults'
ingest_source_locks = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-source-locks'
ingest_source_closure = ingest_retirement or ingest_abandonment or ingest_source_faults or ingest_source_locks or (len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-source-closure')
ingest_recovery_signed = ingest_source_closure or (len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-recovery-signed')
ingest_recovery = ingest_recovery_interruptions or ingest_recovery_signed or (len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-recovery')
ingest_history = len(sys.argv) in (3, 4) and sys.argv[2] == '--ingest-history'
ingest_windows = len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-ingest-windows'
initial_ingest = ingest_activated_acquisition_limits or ingest_activated_acquisition_http or ingest_activated_acquisition_guards or ingest_activated_acquisition_claims or ingest_activated_acquisition_faults or ingest_activated_acquisition or ingest_activated_source_prefix or ingest_activated_source_locks or ingest_activated_source_durable or ingest_activated_source_faults or ingest_activated_source or ingest_activated_refusals or ingest_activated_zero or ingest_activated_faults or ingest_activated_recovery or ingest_installed_live or ingest_live_refusals or ingest_live_routes or ingest_live_guards or ingest_live or installed_empty_retirement or ingest_empty_preparer or ingest_empty_locks or ingest_empty_retirement or ingest_gap_faults or ingest_successor_gap or ingest_successor_history or ingest_succession or ingest_recovery or ingest_history or ingest_windows or (len(sys.argv) in (3, 4) and sys.argv[2] == '--installed-ingest')
installed = installed_archive or installed_sync or jobs_http or scope_http or registration_http or sessions or enrollment_http or initial_ingest
interruptions = len(sys.argv) == 4 and sys.argv[2] == '--installed-interruptions'
commits = len(sys.argv) == 4 and sys.argv[2] == '--installed-commits'
execution = len(sys.argv) == 3 and sys.argv[2] == '--execution'
observation = len(sys.argv) in (3, 4) and sys.argv[2] == '--observation'
socket = len(sys.argv) in (3, 4) and sys.argv[2] == '--socket'
dictionary_faults = len(sys.argv) in (3, 4) and sys.argv[2] == '--dictionary-faults'
assert len(sys.argv) == 2 or registration_faults or guards or refusals or policy or lease or installed or interruptions or commits or execution or observation or socket or dictionary_faults
revision = lab.command(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD']).stdout.strip()
assert not lab.command(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all']).stdout
reuse_build = registration_faults or guards or interruptions or commits or ((installed_archive or installed_sync or initial_ingest or observation or socket or dictionary_faults or sessions or enrollment_http or registration_http or scope_http or jobs_http) and len(sys.argv) == 4)
build = Path(sys.argv[3]).resolve(strict=True) if reuse_build else root / 'out/artifacts'
built = json.loads((build / 'build-result.json').read_text())
assert built['probe_name'] == ('relay_registration_fault_probe' if registration_faults else 'ingest_bootstrap_probe')
if reuse_build:
    # Reuse only reviewed exact artifacts with unchanged production/build inputs.
    # Governance documents are not compiled; retain the original binary identity.
    # Review experiment changes too: this path filter is not a build-input proof.
    changed = lab.command(['git', '-C', str(SOURCE), 'diff', '--name-only', built['source'], revision]).stdout.splitlines()
    assert all(n in ('AGENTS.md', 'PLAN.md') or n.startswith(('docs/', 'experiments/')) for n in changed), changed
else:
    assert built['source'] == revision
sha = lambda value: hashlib.sha256(value).hexdigest()
for name, key in [('bigbrotr', 'binary_sha256'), (built['probe_name'], 'probe_sha256')]:
    assert sha((build / name).read_bytes()) == built[key]
native_inspector = None
if registration_http_faults:
    # Reuse the separately retained, qualified native inspector. No product or
    # codec changed; the later Rust addition is solely the SQL fault example.
    native_inspector = root.parent / 'initial-ingest-ce85c0c/out/artifacts'
    inspected = json.loads((native_inspector / 'build-result.json').read_text())
    assert inspected['source'] == 'ce85c0c223134c83faf68aebb455aa4f014a7509'
    assert inspected['probe_name'] == 'ingest_bootstrap_probe'
    assert sha((native_inspector / 'ingest_bootstrap_probe').read_bytes()) == inspected['probe_sha256']
    changed = lab.command(['git', '-C', str(SOURCE), 'diff', '--name-only', inspected['source'], revision]).stdout.splitlines()
    assert all(n.startswith(('docs/', 'experiments/', 'contracts/', 'packages/api-client/'))
        or n == 'apps/bigbrotr/examples/relay_registration_fault_probe.rs' for n in changed), changed

pg_root = root.parent / 'relay-source-recheck-e06f580/pg'
pg = lab.load(pg_root)
assert pg['owner'] == '5e69b0f060b1607c25230d1dbd3e5608' and pg['state'] == 'stopped-retained'
basis = root.parent / 'acquisition-scope-54d1a18/engine'
basis_report = (basis / 'result.json').read_bytes()
assert json.loads(basis_report)['status'] == 'PASS_SCOPED_ACQUISITION_REGISTRATION'
output = root / 'bindings'
output.mkdir(mode=0o700)
owner, label = secrets.token_hex(16), 'org.bigbrotr.acquisition-binding'
client = ('bigbrotr-scope-command-binding-' if scope_command_faults or scope_http_faults
          else 'bigbrotr-acquisition-binding-') + revision[:7]
report = dict(format='bigbrotr-acquisition-binding/1', source=revision, build=built,
    status='RUNNING', wave='installed-session-hash' if session_hash else 'installed-sessions' if sessions else 'socket' if socket else 'observation' if observation else 'execution' if execution else 'installed-commits' if commits else 'installed-interruptions' if interruptions else 'installed-ingest' if installed else 'ingest-lease' if lease else 'policy' if policy else 'guards' if guards else 'refusals' if refusals else 'first', owner=owner, label=label,
    client=None, cases=[], databases=[],
    postgres=pg['postgres'], postgres_owner=pg['owner'],
    not_run=['installed Sync/progress/M1', 'measured memory capacity/load/aged-state', 'arm64/release']
        + (['Adapter COMMIT/recovery is not repeated in this raw-helper wave; see50b4136 evidence']
           if guards else ['concurrent scope-policy locks and lost COMMIT recovery']))


if installed:
    report['not_run'] = ['native syscall/close delay and worker panic', 'lost/delayed COMMIT and cancellation lock schedules',
        'crash-resumable initialization/successor', 'installed acquisition/M1', 'capacity/load/aged-state', 'arm64/release']

if ingest_recovery:
    report['wave'] = 'ingest-positive-recovery'
    report['not_run'] = ['installed successor/source closure/ACK/M1', 'lost/delayed COMMIT and blocked native syscall', 'capacity/load/aged-state', 'arm64/release']

if ingest_source_closure:
    report['wave'] = 'installed-initial-source-closure'
    report['not_run'] = ['producer/domain retirement, workflow abandonment, successor, ACK/GC and full M1',
        'source-close COMMIT ambiguity/cancellation and blocked native syscall', 'capacity/load/aged-state', 'arm64/release']

if ingest_source_faults:
    report['wave'] = 'installed-initial-source-closure-faults'
    report['not_run'] = ['producer/domain retirement, workflow abandonment, successor, ACK/GC and full M1',
        'concurrent job/source lock schedules and blocked native syscall', 'capacity/load/aged-state', 'arm64/release']

if ingest_source_locks:
    report['wave'] = 'installed-initial-source-closure-locks'
    report['not_run'] = ['producer/domain retirement, workflow abandonment, successor, ACK/GC and full M1',
        'blocked native syscall, capacity/load/aged-state', 'arm64/release',
        'source UPDATE/COMMIT interruption schedules already qualified separately; not repeated']

if ingest_abandonment:
    report['wave'] = 'installed-initial-acquisition-abandonment'
    report['not_run'] = ['producer/domain retirement, successor, ACK/GC and full M1',
        'abandonment interruption/COMMIT schedules and relational negatives',
        'blocked native syscall, capacity/load/aged-state', 'arm64/release']

if ingest_abandonment_guards:
    report['wave'] = 'installed-acquisition-gap-guards'
    report['not_run'] = ['abandonment interruption/COMMIT schedules and64/65 count boundary',
        'producer/domain retirement, successor, ACK/GC and full M1', 'capacity/load/aged-state', 'arm64/release']

if ingest_abandonment_limits:
    report['wave'] = 'installed-acquisition-abandonment-limits'
    report['not_run'] = ['abandonment interruption/COMMIT schedules',
        'producer/domain retirement, successor, ACK/GC and full M1', 'capacity/load/aged-state', 'arm64/release']

if ingest_abandonment_faults:
    report['wave'] = 'installed-acquisition-abandonment-faults'
    report['not_run'] = ['producer/domain retirement, successor, ACK/GC and full M1',
        'blocked native calls/crash/ENOSPC, capacity/load/aged-state', 'arm64/release']

if ingest_retirement:
    report['wave'] = 'installed-initial-retirement-faults' if ingest_retirement_faults else 'installed-initial-retirement'
    report['not_run'] = ['retirement SQL interruption/COMMIT and concurrent lock schedules',
        'native syscall/crash/ENOSPC, capacity/load/aged-state', 'successor, ACK/GC and full M1', 'arm64/release']

if ingest_retirement_faults:
    report['not_run'] = ['retirement current-authority and concurrent lock schedules',
        'zero-issued producer and zero-request import obligations',
        'native syscall/crash/ENOSPC, capacity/load/aged-state',
        'successor, ACK/GC and full M1', 'arm64/release']

if ingest_retirement_locks:
    report['wave'] = 'installed-initial-retirement-locks'
    report['not_run'] = ['Control UTC expiry, dataset/job and standalone producer-row contention',
        'zero-issued producer and zero-request import obligations',
        'native syscall/crash/ENOSPC, capacity/load/aged-state',
        'successor, ACK/GC and full M1', 'arm64/release']

if ingest_retirement_obligations:
    report['wave'] = 'installed-initial-retirement-obligations'
    report['not_run'] = ['Genuine installed import admission/logical closure',
        'True zero-issued installed interrupted startup, Control UTC expiry and other row contention',
        'Native syscall/crash/ENOSPC, capacity/load/aged-state',
        'Successor, ACK/GC and full M1', 'arm64/release']

if ingest_history:
    report['wave'] = 'ingest-history'
    report['not_run'] = ['installed recovery/closing/successor', 'blocked-query cancellation', 'capacity/load/aged-state', 'arm64/release']

if ingest_windows:
    report['wave'] = 'installed-ingest-windows'
    report['not_run'].append('unchanged lifetime/COMMIT/session cases not repeated')

if installed_archive_setup:
    report['wave'] = 'installed-archive-setup'
    report['not_run'] = ['installed Archive publication (baseline role selection refuses)',
        'same-owner signed socket event/archive frontier/read/export/restart/M1',
        'publication crash/COMMIT/cancellation/unknown outcomes', 'capacity/load/aged-state', 'arm64/release']

if installed_archive_publication:
    report['wave'] = 'installed-archive-signed' if installed_archive_signed else 'installed-archive-publication'
    report['not_run'] = ['ordinary serve restart/read/export/M1', 'durable EOSE/progress',
        'publication crash/COMMIT/cancellation/unknown outcomes', 'nine-case Sync wave not repeated',
        'capacity/load/aged-state', 'arm64/release']

if installed_archive_signed:
    report['not_run'].append('three reader startup refusals not repeated in this focused wave')

if installed_archive_export:
    report['wave'] = 'installed-archive-export'
    report['not_run'] = ['M1 successor/replay/indexing', 'five signed exports under256MiB',
        'durable EOSE/progress', 'publication crash/COMMIT/cancellation/unknown outcomes',
        'earlier Sync/reader refusal waves not repeated', 'capacity/load/aged-state', 'arm64/release']

if installed_sync:
    report['wave'] = ('installed-sync-visibility' if installed_sync_visibility
        else 'installed-sync-expiry' if installed_sync_expiry
        else 'installed-sync-window' if installed_sync_window else 'installed-finite-sync')
    report['not_run'] = ['source change before socket', 'missing migration execution refusal',
        'absence of physical WAL appends for redundant native writes',
        'lost/delayed COMMIT and cancellation lock schedules',
        'durable EOSE/progress, restart/archive/read/export/M1',
        'capacity/load/aged-state', 'arm64/release']
    if not installed_sync_expiry:
        report['not_run'].append('older naturally expired accepted job followed by valid work')
    if installed_sync_visibility or installed_sync_expiry or installed_sync_window:
        report['not_run'].append('nine-case installed Sync first wave not repeated in this focused wave')
    if not installed_sync_visibility:
        report['not_run'].append('missing/unsupported native visibility refuses before preparation')
    if installed_sync_expiry:
        report['not_run'].append('expiry without exact-owned process suspension injection')
    if not installed_sync_window:
        report['not_run'].append('full initial producer window and later attribution/event deferral')

if sessions:
    report['not_run'] = ['session listener during blocked native close', 'late/delayed renewal and disconnected clients',
        'worker panic', 'crash-resumable initialization/successor', 'installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']

if session_faults:
    report['wave'] = 'installed-session-faults'
    report['not_run'] = ['worker panic', 'renewal-specific ambiguous COMMIT and policy races',
        'crash-resumable initialization/successor', 'installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']

if enrollment_http:
    report['wave'] = 'installed-relay-enrollment'
    report['not_run'] = ['installed authenticated URL registration', 'CLI enrollment command',
        'concurrent authenticated withdrawal versus source action', 'installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']

if registration_faults:
    report['wave'] = 'registration-authority-races' if registration_races else 'registration-adapter-faults'
    report['not_run'] = ['registration concurrent authority writers and raw relational refusal',
        'historical recovery after changed heads/grants', 'installed HTTP COMMIT ambiguity',
        'installed acquisition/M1', 'capacity/load/aged-state', 'arm64/release']

if registration_races:
    report['not_run'] = ['raw relational refusal/expiry negative controls',
        'installed HTTP COMMIT ambiguity', 'installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']

if registration_relations:
    report['wave'] = 'registration-relational-guards'
    report['not_run'] = ['installed HTTP COMMIT ambiguity', 'installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']

if registration_http_faults:
    report['wave'] = 'installed-registration-signal' if registration_http_signal else 'installed-registration-faults'
    report['native_inspector'] = dict(source=inspected['source'], probe_sha256=inspected['probe_sha256'],
        root=str(native_inspector))
    report['not_run'] = ['installed successor recovery', 'installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']

if registration_http:
    report['wave'] = 'installed-relay-registration'
    report['not_run'] = ['registration adapter COMMIT recovery/cancellation/expiry/concurrency',
        'raw relational refusal/rollback', 'CLI registration command', 'installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']

if scope_http_faults:
    report['wave'] = 'installed-scope-command-faults'
if job_http_faults:
    report['wave'] = 'installed-job-acceptance-faults'
if source_http_regression:
    report['wave'] = 'installed-source-command-regression'

if jobs_http:
    report['wave'] = ('installed-finite-job-expiry' if job_expiry else
        'installed-finite-job-actors' if job_actors else 'installed-finite-acquisition-jobs')
    report['not_run'] = ['installed job COMMIT/cancellation/authority races',
        'session/credential changes' if job_actors else 'foreign actor and session/credential changes',
        'job delivery/execution/progress/recovery/M1', 'capacity/load/aged-state', 'arm64/release']

if job_gap:
    report['wave'] = 'installed-acquisition-gap-compatibility'
    report['not_run'] = ['gap relational negatives and abandonment interruption/COMMIT recovery',
        'producer/domain retirement, successor, ACK/GC and full M1', 'capacity/load/aged-state', 'arm64/release']

if export_status:
    report['wave'] = 'ordinary-export-status-pre0022'
    report['not_run'] = ['positive export row decoding/execution (prior evidence remains separate)',
        'installed acquisition COMMIT/cancellation', 'capacity/load/aged-state', 'arm64/release']

if scope_http:
    report['wave'] = 'installed-acquisition-scope'
    report['not_run'] = ['scope COMMIT recovery/cancellation/expiry/concurrency',
        'raw relational refusal/rollback', 'internal scope adapter regression',
        'finite-job delivery/installed acquisition/M1', 'capacity/load/aged-state', 'arm64/release']

if scope_command_faults:
    report['wave'] = 'scope-command-adapter-faults'
    report['not_run'] = ['scope current-authority races and raw relational refusal',
        'historical recovery after changed heads/grants', 'installed scope HTTP COMMIT ambiguity',
        'internal scope helper regression', 'finite-job delivery/installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']
if scope_command_races:
    report['wave'] = 'scope-command-authority-races'
    report['not_run'] = ['scope raw relational refusal', 'installed scope HTTP COMMIT ambiguity',
        'finite-job delivery/installed acquisition/M1', 'capacity/load/aged-state', 'arm64/release']

if scope_command_relations:
    report['wave'] = 'scope-command-relational-guards'
    report['not_run'] = ['installed scope HTTP COMMIT ambiguity', 'finite-job delivery/installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']

if interruptions:
    report['not_run'] = ['worker panic and arbitrary uninterruptible kernel I/O',
        'lost/delayed COMMIT and cancellation lock schedules', 'crash-resumable initialization/successor',
        'installed acquisition/M1', 'capacity/load/aged-state', 'arm64/release']

if commits:
    report['not_run'] = ['worker panic', 'crash-resumable initialization/successor',
        'renewal-specific lost COMMIT and policy races', 'installed acquisition/M1',
        'capacity/load/aged-state', 'arm64/release']

if lease:
    report['not_run'] = ['installed Ingest/Sync/M1', 'lost or delayed COMMIT',
        'renewal-write old-deadline predicate negative control', 'policy/closing lock races',
        'native worker interruption', 'capacity/load/aged-state', 'arm64/release']


if ingest_succession:
    report['wave'] = 'initial-successor-preparation'
    report['not_run'] = ['process-loss plan reconstruction and abandoned empty attempt retirement',
        'writer activation, lifetime, installed restart/resend and full M1',
        'actual ambiguous preparation COMMIT, process kill and native syscall failure',
        'ACK/GC, capacity/load/aged-state and arm64/release']
if ingest_succession_commits:
    report['wave'] = 'initial-successor-commit-loss'
    report['not_run'] = ['process-loss plan reconstruction and abandoned empty attempt retirement',
        'writer activation, lifetime, installed restart/resend and full M1',
        'recovery while the old backend still holds exclusion; native syscall failure',
        'ACK/GC, capacity/load/aged-state and arm64/release']
if ingest_successor_history:
    report['wave'] = 'prepared-initial-history'
    report['not_run'] = ['PG/native gap completion and abandoned empty attempt retirement',
        'writer activation, lifetime, installed restart/resend and full M1',
        'concurrent live original owner exclusion and native syscall failure',
        'ACK/GC, capacity/load/aged-state and arm64/release']

if ingest_successor_gap:
    report['wave'] = 'prepared-initial-gap-completion'
    report['not_run'] = ['empty attempt retirement, writer activation and installed restart/resend',
        'native syscall uncertainty/power loss, advanced current policy, ACK/GC and full M1',
        'capacity/load/aged-state and arm64/release']

if ingest_gap_faults:
    report['wave'] = 'prepared-gap-policy-delivery'
    report['not_run'] = ['product policy activation, empty attempt retirement and writer activation',
        'native syscall uncertainty/power loss and installed restart/resend',
        'ACK/GC, full M1, capacity/load/aged-state and arm64/release']


if ingest_empty_retirement:
    report['wave'] = 'initial-empty-commit-loss' if ingest_empty_commits else 'initial-empty-retirement'
    report['not_run'] = (['concurrent Control claim/expiry schedules'] if ingest_empty_commits else
        ['empty-retirement COMMIT loss and concurrent Control claim schedules']) + [
        'native syscall uncertainty/power loss, next incarnation activation and installed restart/resend',
        'ACK/GC, full M1, capacity/load/aged-state and arm64/release']


if ingest_empty_locks:
    report['wave'] = 'initial-empty-locks'
    report['not_run'] = ['historical preparer claim requires a separate live preparation fixture',
        'installed import execution; this wave checks the guarded relational claim path',
        'native syscall uncertainty/power loss, next incarnation activation and installed restart/resend',
        'ACK/GC, full M1, capacity/load/aged-state and arm64/release']

if ingest_empty_preparer:
    report['wave'] = 'initial-empty-preparer'
    report['not_run'] = ['installed import execution; this wave checks a live guarded relational claim',
        'native syscall uncertainty/power loss, next incarnation activation and installed restart/resend',
        'ACK/GC, full M1, capacity/load/aged-state and arm64/release']

if installed_empty_retirement:
    report['wave'] = 'installed-empty-retirement'
    report['not_run'] = ['installed CLI COMMIT loss; finite adapter COMMIT loss is separately qualified',
        'blocked native syscalls/power loss, next incarnation activation and full installed restart/resend',
        'installed import execution, ACK/GC, full M1, capacity/load/aged-state and arm64/release']

if ingest_live:
    report['wave'] = 'live-initial-activation'
    report['not_run'] = ['raw SQL activation transaction guards, activated-zero recovery refusal and renewal',
        'real network provenance transitions and installed successor consumers',
        'native syscall uncertainty/power loss, general lifetimes, ACK/GC and full M1',
        'capacity/load/aged-state and arm64/release']

if ingest_live_guards:
    report['wave'] = 'live-initial-guards'
    report['not_run'] = ['renewal refusal/expiry/COMMIT loss and activated-zero old-loader routes',
        'first-request manifest mismatch, real network provenance and installed successor consumers',
        'native syscall uncertainty/power loss, general lifetimes, ACK/GC and full M1',
        'capacity/load/aged-state and arm64/release']

if ingest_activated_acquisition_limits:
    report['wave'] = 'installed-activated-acquisition-limits'
    report['not_run'] = ['final job-wait SIGTERM and actual Control UTC expiry',
        'job completion, producer retirement, Archive restart and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_acquisition_http:
    report['wave'] = 'activated-acquisition-http'
    report['not_run'] = ['combined64 boundary and final job-wait cancellation or actual Control expiry',
        'job completion, producer retirement, Archive restart and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_acquisition_guards:
    report['wave'] = 'activated-acquisition-sql-guards'
    report['not_run'] = ['native-proof dispatch and ordinary HTTP legacy grants',
        'combined64 boundary and final job-wait cancellation or actual Control expiry',
        'producer retirement, Archive restart and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_acquisition_claims:
    report['wave'] = 'installed-activated-acquisition-claims'
    report['not_run'] = ['ordinary HTTP legacy grants, raw SQL guards and combined64 boundary',
        'final job-wait cancellation and actual Control expiry',
        'producer retirement, Archive restart and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_acquisition_faults:
    report['wave'] = 'installed-activated-acquisition-faults'
    report['not_run'] = ['same-session poison or driver-result telemetry',
        'genuine extra zero-request claims, SQL guards and ordinary HTTP legacy grants',
        'producer retirement, Archive restart and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_acquisition:
    report['wave'] = 'installed-activated-acquisition-abandonment'
    report['not_run'] = ['genuine extra zero-request claims and accepted-unclaimed intents',
        'final abandonment COMMIT loss, SQL guards and ordinary HTTP legacy grants',
        'producer retirement, Archive restart and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_source_prefix:
    report['wave'] = 'installed-activated-source-prefix-refusals'
    report['not_run'] = ['final-publication interposition', 'workflow retirement, Archive restart and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_source_locks:
    report['wave'] = 'installed-activated-source-closure-locks'
    report['not_run'] = ['authenticated job acceptance and remaining connected source refusals',
        'workflow retirement, Archive restart and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_source_durable:
    report['wave'] = 'installed-activated-source-already-durable'
    report['not_run'] = ['job/closure lock schedules and remaining connected source refusals',
        'authenticated job completion, workflow retirement, Archive restart and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_source_faults:
    report['wave'] = 'installed-activated-source-closure-faults'
    report['not_run'] = ['same-session poison or driver-result telemetry',
        'connected AlreadyDurable/source-zero and job/closure lock schedules',
        'remaining source closure refusals, workflow retirement, Archive restart and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_source:
    report['wave'] = 'installed-activated-source-closure'
    report['not_run'] = ['connected AlreadyDurable/source-zero, final source COMMIT loss and job/closure lock schedules',
        'remaining source closure corruption/refusal cases, epoch2 workflow abandonment/retirement',
        'repeated lifetimes, Archive restart, ACK/GC and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_refusals:
    report['wave'] = 'installed-activated-recovery-refusals'
    report['not_run'] = ['same-session poison or driver-result telemetry, corruption repair',
        'source2 closure/retirement, repeated lifetimes, Archive restart, ACK/GC and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_zero:
    report['wave'] = 'installed-activated-recovery-source-zero'
    report['not_run'] = ['installed import, network provenance and AlreadyDurable source-zero',
        'corrupt inputs/dependencies, held-native refusal and COMMIT loss in this wave',
        'source2 closure/retirement, repeated lifetimes, Archive restart, ACK/GC and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_faults:
    report['wave'] = 'installed-activated-recovery-faults'
    report['not_run'] = ['driver-result or same-session poison telemetry',
        'issued-positive source-zero, corrupt inputs/dependencies and held-native refusal',
        'source2 closure/retirement, repeated lifetimes, Archive restart, ACK/GC and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_activated_recovery:
    report['wave'] = 'installed-activated-recovery'
    report['not_run'] = ['issued-positive source-zero, corrupt inputs/dependencies and held-native refusal',
        'closing/outcome COMMIT loss and configuration-only recovery',
        'source2 closure/retirement, repeated lifetimes, Archive restart, ACK/GC and full M1',
        'native syscall uncertainty/power loss, capacity/load/aged-state and arm64/release']

if ingest_installed_live:
    report['wave'] = 'installed-live-initial-successor'
    report['not_run'] = ['general activated-owner recovery, repeated lifetimes and Archive restart',
        'installed CLI activation COMMIT loss; finite adapter loss is separately qualified',
        'native syscall uncertainty/power loss, durable coverage, ACK/GC and full M1',
        'capacity/load/aged-state and arm64/release']

if ingest_live_refusals:
    report['wave'] = 'live-initial-refusals'
    report['not_run'] = ['real network provenance and installed successor consumers',
        'native syscall uncertainty/power loss, general lifetimes, ACK/GC and full M1',
        'capacity/load/aged-state and arm64/release']

if ingest_live_routes:
    report['wave'] = 'live-initial-routes'
    report['not_run'] = ['first-request manifest mismatch and renewal refusal/expiry/COMMIT loss',
        'real network provenance and installed successor consumers',
        'native syscall uncertainty/power loss, general lifetimes, ACK/GC and full M1',
        'capacity/load/aged-state and arm64/release']

def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def save():
    lab.write_json(output / 'result.json', report)


def containers():
    return json.loads(lab.docker('inspect', *lab.docker('ps', '-aq').stdout.split()).stdout)


def rows(db):
    names = json.loads(lab.sql(pg, db,
        "SELECT json_agg(tablename ORDER BY tablename) FROM pg_tables WHERE schemaname='control'"))
    require(len(names) in (64, 66, 67, 68, 69, 70, 71, 72) and all(re.fullmatch('[a-z_]+', n) for n in names), 'table-set')
    parts = [f"SELECT '{n}' AS name,coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') AS rows "
             f"FROM (SELECT *,xmin::text AS xmin FROM control.{n}) t" for n in names]
    return json.loads(lab.sql(pg, db, 'SELECT jsonb_object_agg(name,rows) FROM (' + ' UNION ALL '.join(parts) + ') t'))


def physical(db):
    return json.loads(lab.sql(pg, db, """
        SELECT jsonb_object_agg(relname,pg_relation_filenode(c.oid))
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='control' AND c.relkind='r'
    """))


def clone(name, template):
    db = 'bb_scope_bind_' + revision[:7] + '_' + name.replace('-', '_')
    require(re.fullmatch('[a-z0-9_]{1,63}', db), 'database-name')
    lab.sql(pg, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator TEMPLATE {template}')
    report['databases'].append(db)
    save()
    require(rows(db) == rows(template), 'exact-retained-clone')
    return db


def upgrade(db, name, target=21):
    before, files = rows(db), physical(db)
    config = lab.config(output, dict(pg, client=client), db, endpoint='127.0.0.1:5432', boundary='loopback')
    def apply():
        return lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
            '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', str(target))
    result = apply()
    (output / (name + '-migration.stdout')).write_text(result.stdout)
    (output / (name + '-migration.stderr')).write_text(result.stderr)
    require(result.returncode == 0, 'actual-upgrade-' + name)
    after = rows(db)
    stripped = json.loads(json.dumps(after))
    if target == 21:
        for row in stripped['producer_requests']:
            require(row.pop('acquisition_scope_id') is None, 'historical-scope-adoption')
    require(all(stripped[t] == before[t] for t in before if t != 'schema_versions'), 'upgrade-rewrote-old-rows')
    new_versions = [r for r in after['schema_versions'] if r not in before['schema_versions']]
    wanted = ([4] if 'acquisition_scopes' not in before else []) + ([21] if target == 21 else [])
    require(sorted(r['migration'] for r in new_versions) == sorted(wanted), 'upgrade-ledger')
    for row in new_versions:
        filename = {4:'0004_acquisition.sql',21:'0021_acquisition_bindings.sql'}[row['migration']]
        require(row['checksum'] == '\\x' + sha((SOURCE / 'migrations/postgres' / filename).read_bytes()), 'upgrade-checksum')
    require(all(row in after['schema_versions'] for row in before['schema_versions']), 'old-ledger-changed')
    current_files = physical(db)
    require(all(current_files[t] == value for t, value in files.items()), 'nullable-column-heap-rewrite')
    require(apply().returncode == 0 and rows(db) == after and physical(db) == current_files, 'reapply-mutated-data')
    lab.write_json(output / (name + '-upgrade.json'), dict(before=before, after=after, physical_before=files, physical_after=current_files))
    lab.sql(pg, db, 'GRANT SELECT,INSERT ON control.acquisition_selectors,control.acquisition_scopes TO bigbrotr_control; '
        'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control;')
    return before, after


def old_source_schema():
    db = 'bb_scope_bind_' + revision[:7] + '_old_source_schema'
    lab.sql(pg, 'postgres', f'CREATE DATABASE {db} OWNER bigbrotr_migrator')
    report['databases'].append(db)
    save()
    config = lab.config(output, dict(pg, client=client), db, endpoint='127.0.0.1:5432', boundary='loopback')
    result = lab.cli(dict(pg, client=client), 'migrate', 'apply', '--file', config,
        '--user', 'bigbrotr_migrator', '--password-ref', 'file:/run/secrets/migrator', '--target', '19')
    require(result.returncode == 0, 'old-source-schema-setup')
    local = output / Path(config).name
    local.write_text(lab.change(local.read_text(), 'postgres.control', 'password_ref', 'file:/run/secrets/application'))
    lab.sql(pg, db, 'GRANT USAGE ON SCHEMA control TO bigbrotr_control; '
        'GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA control TO bigbrotr_control; '
        'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA control TO bigbrotr_control; '
        'GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs TO bigbrotr_control;')
    for args in [('config','initialize'), ('config','activate-initial','--request-id','77'*16,'--expected-revision','1')]:
        result = lab.cli(dict(pg, client=client), *args, '--file', config)
        require(result.returncode == 0, 'old-source-' + args[1])
    lab.sql(pg, db, """
        INSERT INTO control.principals(principal_id,dataset_id,kind,created_at_us,database_role,local_os_uid)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'local',0,'fixture',0 FROM control.datasets;
        INSERT INTO control.local_accounts(principal_id,dataset_id,username,initial_owner,active,credential_revision,grant_revision)
            SELECT decode(repeat('10',16),'hex'),dataset_id,'alice',true,true,1,1 FROM control.datasets;
        INSERT INTO control.local_credentials(principal_id,dataset_id,credential_revision,format,argon_version,memory_kib,passes,lanes,salt,password_hash)
            SELECT principal_id,dataset_id,1,'argon2id-v1',19,65536,3,1,decode(repeat('02',16),'hex'),
                decode('a9f16b27ee4188b48954ee896ca04320bdbe34f4c2c0b9f4f332d9daffc892ff','hex') FROM control.local_accounts;
        INSERT INTO control.principal_grants SELECT dataset_id,principal_id,'policy.change','dataset',dataset_id FROM control.local_accounts;
    """)
    return db


def rejected_source(dataset):
    """Literal forged-signature request/rejection; no product codec or dump input."""
    native, inputs, _ = complete_source(dataset, False)
    event = json.loads((SOURCE / 'tests/fixtures/ingest/import-event.jsonl').read_bytes())
    be = lambda n: n.to_bytes(8, 'big')
    slot = bytes([66]) * 16
    visibility = [dataset, 4, bytes([64]) * 16, 1]
    observer = [dataset, 2, bytes([65]) * 16, 1]
    body = [bytes.fromhex(event['id']), bytes.fromhex(event['pubkey']), event['created_at'],
            event['kind'], event['tags'], event['content'], bytes(64)]
    inputs[2] = b'bigbrotr/producer-request/1\0' + oracle.cbor([
        [slot, 1, 2], 'admit', 1, [1, visibility, body, 'network', None,
        [456, observer, [observer, 1, None, 0], None]]])
    rejection = (b'bigbrotr/rejection/1\0' + slot + be(1) + be(2)
                 + hashlib.sha256(inputs[2]).digest() + dataset + be(1) + b'\x04')
    marker = b'\x01\x31' + slot + be(1)
    native = [r for r in native if (r[0], r[1]) != ('receipts', marker)]
    native += [('receipts', marker, b'\x01\x31' + be(2) + be(0) + be(0)),
               ('receipts', b'\x01\x30' + slot + be(1) + be(2), rejection)]
    return sorted(native), inputs, rejection


def refuse_dispatch(db):
    # Key the fault to an existing durable scoped reservation, not call counts.
    # The actual reservation trigger still checks the real current scope first.
    lab.sql(pg, db, """
        ALTER FUNCTION control.require_acquisition_scope(bytea,bytea,bytea,bytea)
            RENAME TO require_acquisition_scope_original;
        CREATE FUNCTION control.require_acquisition_scope(d bytea,s bytea,e bytea,a bytea)
        RETURNS void LANGUAGE plpgsql AS $$ BEGIN
            IF EXISTS(SELECT 1 FROM control.producer_requests
                WHERE dataset_id=d AND acquisition_scope_id=s) THEN
                RAISE EXCEPTION 'acquisition scope is not current for this source'
                    USING ERRCODE='23514';
            END IF;
            PERFORM control.require_acquisition_scope_original(d,s,e,a);
        END $$;
        REVOKE ALL ON FUNCTION control.require_acquisition_scope(bytea,bytea,bytea,bytea) FROM PUBLIC;
        GRANT EXECUTE ON FUNCTION control.require_acquisition_scope(bytea,bytea,bytea,bytea) TO bigbrotr_control;
    """)


def arrival_policy(db, name):
    """Independent policy documents injected into this fresh clone before ownership.

    This is no management activation/transition claim. The original template and
    every applied migration remain unchanged; neither expected bytes nor policy
    fingerprints call the production codec.
    """
    before = rows(db)
    require(not before['owner_epochs'] and not before['producer_requests'], 'policy-fixture-not-empty')
    expected = json.loads(json.dumps(before))
    mutations = []
    for table in ['config_revisions', 'policy_revisions']:
        require(len(before[table]) == 1, 'policy-fixture-single-revision')
        document = json.loads((SOURCE / 'experiments/configuration-inspection/policy.json').read_text())
        document['format'] = 'bigbrotr-product-policy/2'
        settings = document['settings']
        settings['relay_policy.banned'] = []
        settings['sync.provenance'] = 'off'
        if table == 'config_revisions':
            if name == 'policy-provenance':
                settings['sync.provenance'] = 'local_first_witness'
            if name == 'policy-limit':
                # Keep import.max_event_bytes <= ingest.max_event_bytes so
                # full restoration succeeds and only the prepared limit fails.
                settings['ingest.max_event_bytes'] = 524288
            if name == 'policy-floor':
                settings['sync.history_since'] = 1
            if name == 'policy-ephemeral':
                settings['sync.ephemeral'] = 'drop'
        elif name == 'policy-second':
            settings['sync.provenance'] = 'local_first_witness'
        encoded = json.dumps(document, separators=(',', ':'), sort_keys=True, ensure_ascii=False).encode()
        digest = sha(b'bigbrotr/product-policy/2\0' + encoded)
        expected[table][0].update(document=document, document_digest='\\x' + digest, schema_version=2)
        mutations.append(f"UPDATE control.{table} SET document=convert_from(decode('{encoded.hex()}','hex'),'UTF8')::jsonb, "
                         f"document_digest=decode('{digest}','hex'),schema_version=2")
    lab.sql(pg, db, 'BEGIN; SET LOCAL session_replication_role=replica; ' + '; '.join(mutations) + '; COMMIT;')
    after = rows(db)
    comparable = json.loads(json.dumps(after))
    for table in ['config_revisions', 'policy_revisions']:
        comparable[table][0]['xmin'] = before[table][0]['xmin']
    require(comparable == expected, 'policy-fixture-unexpected-effects')
    lab.write_json(output / (name + '-policy-setup.json'), dict(before=before, after=after))


def run_case(name):
    db = old_source_schema() if name == 'old-source-schema' else clone(name, 'bb_source_template')
    if name != 'old-source-schema':
        upgrade(db, name, target=4 if name == 'old-binding-schema' else 21)
    if name == 'dispatch-refused':
        refuse_dispatch(db)
    if policy:
        arrival_policy(db, name)
    if guards:
        fixture = SOURCE / 'experiments/acquisition-scope/binding_guards.sql'
        report['guard_fixture_sha256'] = sha(fixture.read_bytes())
        lab.sql(pg, db, fixture.read_text())
    before = rows(db)
    require(not before['producer_requests'] and not before['owner_epochs'], 'fresh-consumer-prerequisite')
    lab.write_json(output / (name + '-before.json'), before)
    mode = ('source-arrival-' if policy else 'source-scope-') + name
    result = lab.docker('exec', client, '/build/ingest_bootstrap_probe', mode, db,
        '/run/secrets/application', '/lab/' + name, check=False, timeout=90)
    (output / (name + '.stdout')).write_text(result.stdout)
    (output / (name + '.stderr')).write_text(result.stderr)
    require(result.returncode == 0 and not result.stderr, 'actual-scope-adapter-' + name)
    messages = [json.loads(line) for line in result.stdout.splitlines()]
    require(messages[-1]['event'] == 'scope-finished' and messages[-1]['mode'] == 'source-scope-' + name
        and messages[-1]['status'] == 'PASS'
        and messages[-1]['zero_writes'] == (name not in ('invalid', 'dispatch-refused')), 'phase-state-comparison')
    after = rows(db)
    lab.write_json(output / (name + '-after.json'), after)
    if policy:
        decisions = [m for m in messages if m['event'] == 'arrival-policy']
        if name.startswith('policy-'):
            expected_count = 2 if name == 'policy-second' else 1
            require(len(decisions) == expected_count and not decisions[-1]['accepted']
                and all(m['accepted'] for m in decisions[:-1]), 'policy-refusal-phase')
            require(all(m['operation'] == 'admission' for m in decisions), 'policy-operation')
            expected_digests = [before[t][0]['document_digest'][2:] for t in ['config_revisions', 'policy_revisions']]
            require([m['digest'] for m in decisions] == expected_digests[:expected_count], 'actual-accepted-documents')
        else:
            require(decisions and all(m['accepted'] for m in decisions), 'unexpected-arrival-policy-refusal')
            require(any(m['operation'] == 'admission' for m in decisions), 'missing-admission-policy')
            if name in ('pass', 'invalid-retained'):
                require(any(m['operation'] == 'certification' for m in decisions), 'missing-certification-policy')
        for table in ['config_revisions', 'policy_revisions']:
            require(after[table] == before[table], 'accepted-policy-mutated')
    committed = name in ('pass','changed','omitted','legacy-adopt','certify-withdrawn','invalid-retained')
    dataset = bytes.fromhex(after['datasets'][0]['dataset_id'][2:])
    expected, inputs, receipts = complete_source(dataset, committed)
    if name == 'invalid':
        expected, inputs, rejection = rejected_source(dataset)
        require(messages[-1]['retry_zero_writes'], 'missing-rejection-retry')
        outcome = next(r for r in after['producer_outcomes'] if r['request_sequence'] == 2)
        require(outcome['outcome_bytes'] == '\\x' + rejection.hex()
            and outcome['outcome_sha256'] == '\\x' + sha(rejection), 'exact-PG-rejection')
    if name == 'dispatch-refused':
        _, inputs, _ = complete_source(dataset, True)
        first = next(m['state'] for m in messages if m['event'] == 'scope-before')
        last = messages[-1]['state']
        states = [{n: json.loads(v) for n, v in state['control_tables']} for state in (first, last)]
        old, new = states
        require({n for n in old if old[n] != new[n]} == {'producer_requests', 'producer_epochs'}, 'dispatch-effects')
        require(len(new['producer_requests']) == 2 and old['producer_requests'][0] in new['producer_requests'], 'dispatch-reservation')
        previous, current = [dict(t['producer_epochs'][0]) for t in states]
        require(previous.pop('reserved_through_request') == 1 and current.pop('reserved_through_request') == 2, 'dispatch-reserved-prefix')
        previous.pop('xmin'); current.pop('xmin')
        require(previous == current and len(new['producer_epochs']) == 1, 'dispatch-producer-mutation')
    dumped = lab.docker('exec', client, '/build/ingest_bootstrap_probe', 'dump', '/lab/' + name)
    (output / (name + '-native.json')).write_text(dumped.stdout)
    actual = [(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in json.loads(dumped.stdout)['rows']]
    require(actual == expected, 'independent-complete-native-bytes-' + name)
    originals = sorted((output / name / 'producer-inputs-v1').glob('*.cbor'))
    require(len(originals) == len(inputs) and all(p.read_bytes() == inputs[int(p.stem[-16:],16)] for p in originals), 'exact-original-inputs')
    require(len(after['producer_requests']) == len(inputs)
        and len(after['producer_outcomes']) == (1 if name == 'dispatch-refused' else len(inputs)), 'exact-operational-counts')
    for row in after['producer_requests']:
        sequence = int(row['request_sequence'])
        require(row['request_digest'] == '\\x' + sha(inputs[sequence]) and row['input_bytes'] == len(inputs[sequence]), 'original-request-binding')
        if 'acquisition_scope_id' in row:
            if sequence == 1 or name == 'legacy-adopt':
                require(row['acquisition_scope_id'] is None, 'legacy-row-adopted')
            else:
                identities = next(m for m in messages if m['event'] == 'scope-identities')
                require(row['acquisition_scope_id'] == '\\x' + identities['original'], 'changed-reservation-scope')
                scope = next(s for s in after['acquisition_scopes'] if s['scope_id'] == row['acquisition_scope_id'])
                require([row[k] for k in ['dataset_id','relay_endpoint_id','relay_access_scope_id']] ==
                    [scope[k] for k in ['dataset_id','endpoint_id','access_scope_id']], 'complete-source-fk')
    if name != 'old-source-schema':
        documents = {r['document'] for r in after['acquisition_selectors']}
        kinds = [2] if name == 'outside-collection' else [1]
        def canonical(values):
            return '\\x' + json.dumps(dict(version=1,filters=[dict(kinds=values)],history_since=0,ephemeral='retain'),separators=(',',':')).encode().hex()
        require(documents == {canonical(kinds), canonical([1,2])} and len(after['acquisition_scopes']) == 2, 'literal-retained-selectors')
    if name == 'pass':
        require(any(m.get('event') == 'scope-needs-admission' and m['zero_writes'] for m in messages), 'read-before-admission')
    if name in ('certify-withdrawn', 'invalid-withdrawn'):
        require(sum(not r['enabled'] and r['generation'] == 2 for r in after['relay_enrollments']) == 1, 'actual-withdrawal')
    require(lab.sql(pg, db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='bigbrotr_control'") == '0', 'unjoined-driver')
    if guards:
        checks = json.loads(lab.sql(pg, db, 'SELECT json_agg(r ORDER BY position) FROM binding_fixture.results r'))
        require(len(checks) == 23 and all(c['unchanged'] for c in checks)
            and len({c['backend_pid'] for c in checks}) == 1
            and [c['position'] for c in checks] == list(range(1,24)), 'nested-SQL-guards')
        require(sum(c['sqlstate'] == 'ZX001' for c in checks) == 5
            and sum(c['sqlstate'] == '23514' for c in checks) == 18, 'guard-control-counts')
        lab.write_json(output / 'relational-checks.json', checks)
        report['relational_checks'] = len(checks)
    report['cases'].append(dict(name=name, status='PASS', actual_pg_rocks=True, complete_native_rows=len(actual), original_inputs=len(inputs)))
    save()
    return db


success_status = 'PASS_INITIAL_SESSION_HASH_CASE' if session_hash else 'PASS_INITIAL_SESSION_FIRST_WAVE' if sessions else 'PASS_SOCKET_ARRIVAL_FIRST_WAVE' if socket else 'PASS_COMMITTED_OBSERVATION_FIRST_WAVE' if observation else 'PASS_ACQUISITION_EXECUTION_FIRST_WAVE' if execution else 'PASS_INITIAL_INGEST_COMMIT_WAVE' if commits else 'PASS_INITIAL_INGEST_INTERRUPTION_WAVE' if interruptions else 'PASS_INITIAL_INGEST_LIFETIME_FIRST_WAVE' if installed else 'PASS_INITIAL_INGEST_LEASE' if lease else 'PASS_SCOPED_ACQUISITION_BINDING'
if installed_archive_setup:
    success_status = 'PASS_INSTALLED_ARCHIVE_SETUP_EXPECTED_ROLE_REFUSAL'
if ingest_recovery:
    success_status = 'PASS_INSTALLED_POSITIVE_RECOVERY'
if ingest_source_closure:
    success_status = 'PASS_INSTALLED_INITIAL_SOURCE_CLOSURE'
if ingest_source_faults:
    success_status = 'PASS_INSTALLED_INITIAL_SOURCE_CLOSURE_FAULTS'
if ingest_source_locks:
    success_status = 'PASS_INSTALLED_INITIAL_SOURCE_CLOSURE_LOCKS'
if ingest_abandonment:
    success_status = ('PASS_INSTALLED_ACQUISITION_ABANDONMENT_FAULTS' if ingest_abandonment_faults
        else 'PASS_INSTALLED_ACQUISITION_ABANDONMENT_LIMITS' if ingest_abandonment_limits
        else 'PASS_INSTALLED_ACQUISITION_GAP_GUARDS' if ingest_abandonment_guards
        else 'PASS_INSTALLED_INITIAL_ACQUISITION_ABANDONMENT_FIRST_WAVE')
if ingest_retirement:
    success_status = ('PASS_INSTALLED_INITIAL_RETIREMENT_OBLIGATIONS' if ingest_retirement_obligations
        else 'PASS_INSTALLED_INITIAL_RETIREMENT_LOCKS' if ingest_retirement_locks
        else 'PASS_INSTALLED_INITIAL_RETIREMENT_FAULTS' if ingest_retirement_faults
        else 'PASS_INSTALLED_INITIAL_RETIREMENT_FIRST_WAVE')
if ingest_history:
    success_status = 'PASS_HISTORICAL_INGEST_LOOKUP'
if ingest_windows:
    success_status = 'PASS_INSTALLED_INGEST_WINDOWS'
if installed_archive_publication:
    success_status = 'PASS_INSTALLED_ARCHIVE_PUBLICATION'
if installed_archive_export:
    success_status = 'PASS_INSTALLED_ARCHIVE_EXPORT'
if installed_sync:
    success_status = ('PASS_INSTALLED_SYNC_VISIBILITY' if installed_sync_visibility
        else 'PASS_INSTALLED_SYNC_NATURAL_EXPIRY' if installed_sync_expiry
        else 'PASS_INSTALLED_SYNC_FULL_WINDOW' if installed_sync_window
        else 'PASS_INSTALLED_FINITE_SYNC_FIRST_WAVE')
if session_faults:
    success_status = 'PASS_INITIAL_SESSION_FAULT_WAVE'
if enrollment_http:
    success_status = 'PASS_INITIAL_ENROLLMENT_HTTP_WAVE'

if registration_faults:
    success_status = 'PASS_REGISTRATION_AUTHORITY_RACES' if registration_races else 'PASS_REGISTRATION_COMMIT_FAULTS'

if registration_relations:
    success_status = 'PASS_REGISTRATION_RELATIONAL_GUARDS'

if registration_http_faults:
    success_status = 'PASS_INSTALLED_REGISTRATION_FAULTS'

if registration_http:
    success_status = 'PASS_INITIAL_REGISTRATION_HTTP_WAVE'
if jobs_http:
    success_status = 'PASS_INITIAL_FINITE_JOB_HTTP_WAVE'
if job_gap:
    success_status = 'PASS_INITIAL_FINITE_JOB_GAP_COMPATIBILITY'
if export_status:
    success_status = 'PASS_PRE0022_EXPORT_STATUS_QUERY'
if scope_http:
    success_status = 'PASS_INITIAL_SCOPE_HTTP_WAVE'
if scope_command_faults:
    success_status = 'PASS_SCOPE_COMMAND_ADAPTER_FAULTS'
if scope_command_races:
    success_status = 'PASS_SCOPE_COMMAND_AUTHORITY_RACES'

if scope_http_faults:
    success_status = 'PASS_INSTALLED_SCOPE_COMMAND_FAULTS'
if job_http_faults:
    success_status = 'PASS_INSTALLED_JOB_ACCEPTANCE_FAULTS'
if source_http_regression:
    success_status = 'PASS_INSTALLED_SOURCE_COMMAND_REGRESSION'

if scope_command_relations:
    success_status = 'PASS_SCOPE_COMMAND_RELATIONAL_GUARDS'
if dictionary_faults:
    report['wave'] = 'dictionary-append-faults'
    success_status = 'PASS_DICTIONARY_APPEND_COMMIT_FAULTS'

if ingest_succession:
    success_status = 'PASS_INITIAL_SUCCESSOR_COMMIT_LOSS' if ingest_succession_commits else 'PASS_INITIAL_SUCCESSOR_PREPARATION'
if ingest_successor_history:
    success_status = 'PASS_PREPARED_INITIAL_HISTORY'

if ingest_successor_gap:
    success_status = 'PASS_PREPARED_INITIAL_GAP_COMPLETION'

if ingest_gap_faults:
    success_status = 'PASS_PREPARED_GAP_POLICY_DELIVERY'

if installed_empty_retirement:
    success_status = 'PASS_INSTALLED_EMPTY_RETIREMENT'

if ingest_empty_preparer:
    success_status = 'PASS_INITIAL_EMPTY_PREPARER_CLAIM'

if ingest_empty_locks:
    success_status = 'PASS_INITIAL_EMPTY_RETIREMENT_LOCKS'

if ingest_empty_retirement:
    success_status = 'PASS_INITIAL_EMPTY_RETIREMENT_COMMIT_LOSS' if ingest_empty_commits else 'PASS_INITIAL_EMPTY_RETIREMENT'

if ingest_live:
    success_status = 'PASS_LIVE_INITIAL_ACTIVATION_FINITE'

if ingest_live_guards:
    success_status = 'PASS_LIVE_INITIAL_GUARDS'

if ingest_activated_acquisition_limits:
    success_status = 'PASS_INSTALLED_ACTIVATED_ACQUISITION_LIMITS'

if ingest_activated_acquisition_http:
    success_status = 'PASS_ACTIVATED_ACQUISITION_HTTP_LEGACY_GRANTS'

if ingest_activated_acquisition_guards:
    success_status = 'PASS_ACTIVATED_ACQUISITION_SQL_GUARDS'

if ingest_activated_acquisition_claims:
    success_status = 'PASS_INSTALLED_ACTIVATED_ACQUISITION_CLAIMS'

if ingest_activated_acquisition_faults:
    success_status = 'PASS_INSTALLED_ACTIVATED_ACQUISITION_ABANDONMENT_COMMIT_LOSS'

if ingest_activated_acquisition:
    success_status = 'PASS_INSTALLED_ACTIVATED_ACQUISITION_ABANDONMENT_FIRST_WAVE'

if ingest_activated_source_prefix:
    success_status = 'PASS_INSTALLED_ACTIVATED_SOURCE_PREFIX_REFUSALS'

if ingest_activated_source_locks:
    success_status = 'PASS_INSTALLED_ACTIVATED_SOURCE_CLOSURE_LOCKS'

if ingest_activated_source_durable:
    success_status = 'PASS_INSTALLED_ACTIVATED_SOURCE_ALREADY_DURABLE'

if ingest_activated_source_faults:
    success_status = 'PASS_INSTALLED_ACTIVATED_SOURCE_CLOSURE_COMMIT_LOSS'

if ingest_activated_source:
    success_status = 'PASS_INSTALLED_ACTIVATED_SOURCE_CLOSURE_FIRST_WAVE'

if ingest_activated_refusals:
    success_status = 'PASS_INSTALLED_ACTIVATED_RECOVERY_REFUSALS'

if ingest_activated_zero:
    success_status = 'PASS_INSTALLED_ACTIVATED_RECOVERY_SOURCE_ZERO'

if ingest_activated_faults:
    success_status = 'PASS_INSTALLED_ACTIVATED_RECOVERY_COMMIT_LOSS'

if ingest_activated_recovery:
    success_status = 'PASS_INSTALLED_ACTIVATED_RECOVERY'

if ingest_installed_live:
    success_status = 'PASS_INSTALLED_LIVE_INITIAL_SUCCESSOR'

if ingest_live_refusals:
    success_status = 'PASS_LIVE_INITIAL_REFUSALS'

if ingest_live_routes:
    success_status = 'PASS_LIVE_INITIAL_ROUTES'

prior = containers()
target = next(c for c in prior if c['Name'] == '/' + pg['postgres'])
assert target['Config']['Labels'][lab.LABEL] == pg['owner'] and not target['State']['Running']
assert not any(c['State']['Running'] and 'bigbrotr' in c['Name'] for c in prior)
assert lab.docker('inspect', client, check=False).returncode != 0
report.update(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    postgres_id=target['Id'], free_bytes_before=shutil.disk_usage(root).free)
assert report['free_bytes_before'] > 2 * 1024**3
if installed_archive_export:
    assert report['free_bytes_before'] > 64 * 1024**3 + 577 * 1024**2, 'export completion headroom'
lab.write_json(output / 'preflight.json', dict(containers=[projection(c) for c in prior],
    processes=lab.command(['ps', '-eo', 'pid,ppid,stat,comm']).stdout,
    ports=lab.command(['ss', '-lntup']).stdout, volumes=lab.docker('volume', 'ls').stdout,
    directories=sorted(p.name for p in root.parent.iterdir())))
save()
try:
    instrument_mount = []
    if interruptions or session_faults:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from interruptions import prepare_instrument
        instrument = prepare_instrument(root, prior, SOURCE)
        instrument_mount = ['--mount', f'type=bind,src={instrument},dst=/instrument,readonly']
    if commits or registration_faults or dictionary_faults or ingest_gap_faults or ingest_empty_locks:
        instrument_mount = ['--mount', f'type=bind,src={SOURCE},dst=/source,readonly']
    if native_inspector is not None:
        instrument_mount += ['--mount', f'type=bind,src={native_inspector},dst=/native-inspect,readonly']
    lab.docker('start', target['Id'])
    deadline = time.monotonic() + 30
    while lab.docker('exec', target['Id'], 'pg_isready', '-U', 'postgres', check=False).returncode:
        require(time.monotonic() < deadline, 'postgres-readiness')
        time.sleep(.1)
    basic = rows('bb_scope_54d1a18_basic')
    template = rows('bb_source_template')
    require(basic == json.loads((basis / 'basic-after.json').read_text()), 'original-scope-basis')
    require(not template['producer_requests'] and not template['owner_epochs'], 'original-template-preconditions')
    registration_basis = ('bb_scope_bind_45f7593_registration_journey' if registration_faults or scope_http
        else 'bb_scope_bind_917c76b_enrollment_journey')
    if scope_command_faults:
        registration_basis = 'bb_scope_bind_5e7dbab_scope_journey'
    if registration_http or registration_faults or scope_http:
        registration_original = dict(database=registration_basis,
            rows=rows(registration_basis), physical=physical(registration_basis))
        require(registration_original['rows']['relay_enrollment_changes'], 'populated-registration-basis')
        if registration_faults or scope_http:
            original_capture = root.parent / 'initial-registration-45f7593/bindings/registration-journey-after.json'
            if scope_command_faults:
                original_capture = root.parent / 'initial-scopes-5e7dbab/bindings/scope-journey-after.json'
            require(registration_original['rows'] == json.loads(original_capture.read_text()),
                'retained-registration-basis-differs-from-qualified-capture')
        lab.write_json(output / 'registration-basis-before.json', registration_original)
    ident = lab.docker('create', '--init', '--name', client, '--label', label + '=' + owner,
        '--network', 'container:' + pg['postgres'], '--user', f'{os.getuid()}:{os.getgid()}',
        '--cpus', '2', '--memory', '3g' if installed_archive else '1g',
        '--memory-swap', '3g' if installed_archive else '1g', '--pids-limit', '128', '--read-only',
        '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
        '--mount', f'type=bind,src={output},dst=/lab',
        '--mount', f'type=bind,src={pg_root}/secrets,dst=/run/secrets,readonly',
        '--mount', f'type=bind,src={build},dst=/build,readonly', *instrument_mount, pg['client_image'],
        'python3', '-c', 'import time; time.sleep(600)').stdout.strip()
    report['client'] = dict(id=ident, name=client)
    save()
    lab.docker('start', ident)
    if registration_faults:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from registration_faults import CASES as COMMIT_CASES, run as run_registration_fault
        registration_runs = {case: run_registration_fault for case in COMMIT_CASES}
        if registration_races:
            from registration_races import CASES as RACE_CASES, run as run_registration_race
            require(not set(COMMIT_CASES) & set(RACE_CASES), 'duplicate-registration-case')
            registration_runs.update({case: run_registration_race for case in RACE_CASES})
        if registration_relations:
            from registration_relations import CASES as RELATIONAL_CASES, run as run_registration_relation
            registration_runs = {case: run_registration_relation for case in RELATIONAL_CASES}
        if registration_http_faults:
            from registration_http_faults import CASES as HTTP_CASES, run as run_registration_http_fault
            registration_runs = {case: run_registration_http_fault for case in HTTP_CASES}
            if scope_http_faults:
                from functools import partial
                from scope_http_faults import CASES as SCOPE_HTTP_CASES
                registration_runs = {case: partial(run_registration_http_fault, scope=True) for case in SCOPE_HTTP_CASES}
            if job_http_faults:
                from functools import partial
                from job_http_faults import CASES as JOB_HTTP_CASES
                registration_runs = {case: partial(run_registration_http_fault, job=True) for case in JOB_HTTP_CASES}
            if source_http_regression:
                from functools import partial
                registration_runs = {'register-http-pass': run_registration_http_fault,
                    'scope-http-pass': partial(run_registration_http_fault, scope=True)}
            if registration_http_signal:
                registration_runs = {'register-http-signal': run_registration_http_fault}
        if scope_command_faults:
            from functools import partial
            from scope_command_faults import CASES as SCOPE_CASES
            registration_runs = {case: partial(run_registration_fault, scope=True) for case in SCOPE_CASES}
        if scope_command_relations:
            from scope_command_relations import CASES as SCOPE_RELATIONAL_CASES
            registration_runs = {case: partial(run_registration_relation, scope=True) for case in SCOPE_RELATIONAL_CASES}
        if scope_command_races:
            from registration_races import SCOPE_CASES as SCOPE_RACE_CASES, run as run_scope_race
            registration_runs = {case: partial(run_scope_race, scope=True) for case in SCOPE_RACE_CASES}
        cases = list(registration_runs)
        require(len(cases) == (2 if source_http_regression else 11 if scope_command_races else 1 if registration_http_signal else 5 if registration_http_faults else 1 if registration_relations else 18 if registration_races else 7),
            'registration-case-set')
    elif dictionary_faults:
        from dictionary_faults import run as run_dictionary_faults
        run_dictionary_faults(globals())
        cases = []
    elif socket:
        from socket_arrival import run as run_socket
        run_socket(globals())
        cases = []
    elif observation:
        from observations import run as run_observation
        run_observation(globals())
        cases = []
    elif execution:
        from execution import run as run_execution
        run_execution(globals())
        cases = []
    elif commits:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from commits import CASES, run as run_commits
        cases = list(CASES)
    elif interruptions:
        cases = ['trace-pass','initialize-signal','initialize-deadline','close-signal']
    elif export_status:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from control_status import CASES
        cases = list(CASES)
    elif installed_archive_publication:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from installed_archive_journey import CASES as ARCHIVE_PUBLICATION_CASES
        ARCHIVE_PUBLICATION_CASES = ['archive-signed'] if installed_archive_signed else ARCHIVE_PUBLICATION_CASES
        if installed_archive_export:
            ARCHIVE_PUBLICATION_CASES = ['archive-export']
        cases = list(ARCHIVE_PUBLICATION_CASES)
    elif installed_archive_setup:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from installed_archive import CASES as ARCHIVE_SETUP_CASES
        cases = list(ARCHIVE_SETUP_CASES)
    elif installed_sync:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from installed_sync import CASES, EXPIRY_CASES, VISIBILITY_CASES, WINDOW_CASES
        selected_sync_cases = (VISIBILITY_CASES if installed_sync_visibility
            else EXPIRY_CASES if installed_sync_expiry
            else WINDOW_CASES if installed_sync_window else CASES)
        cases = list(selected_sync_cases)
    elif jobs_http:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from initial_jobs import ACTOR_CASES, CASES, EXPIRY_CASES, GAP_CASES
        cases = (['jobs-journey', *GAP_CASES, 'sync-signed', 'sync-gap-uncorrected'] if job_gap
            else list(EXPIRY_CASES if job_expiry else ACTOR_CASES if job_actors else CASES))
    elif scope_http:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from scopes import CASES
        cases = list(CASES)
    elif registration_http:
        sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
        from registrations import CASES
        cases = list(CASES)
    elif enrollment_http:
        cases = (['enrollment-function-route', 'enrollment-function-digest', 'enrollment-function-u64', 'enrollment-journey']
            if enrollment_functions else ['enrollment-journey', 'enrollment-permission', 'enrollment-prerequisite'])
    elif sessions:
        if session_faults:
            cases = ['sessions-slow-renewal', 'sessions-disconnect', 'close-signal']
        else:
            cases = ['sessions-hash-stop'] if session_hash else ['sessions-journey', 'sessions-capacity', 'sessions-hash-stop']
    elif ingest_activated_acquisition_limits:
        cases = ['installed-activated-acquisition-limits']
    elif ingest_activated_acquisition_http:
        cases = ['activated-acquisition-http']
    elif ingest_activated_acquisition_guards:
        cases = ['activated-acquisition-sql-guards']
    elif ingest_activated_acquisition_claims:
        cases = ['installed-activated-acquisition-claims']
    elif ingest_activated_acquisition_faults:
        cases = ['installed-activated-acquisition-faults']
    elif ingest_activated_acquisition:
        cases = ['installed-activated-acquisition-abandonment']
    elif ingest_activated_source_prefix:
        cases = ['installed-activated-source-prefix-refusals']
    elif ingest_activated_source_locks:
        cases = ['installed-activated-source-closure-locks']
    elif ingest_activated_source_durable:
        cases = ['installed-activated-source-already-durable']
    elif ingest_activated_source_faults:
        cases = ['installed-activated-source-closure-faults']
    elif ingest_activated_source:
        cases = ['installed-activated-source-closure']
    elif ingest_activated_refusals:
        cases = ['installed-activated-recovery-refusals']
    elif ingest_activated_zero:
        cases = ['installed-activated-recovery-source-zero']
    elif ingest_activated_faults:
        cases = ['installed-activated-recovery-faults']
    elif ingest_activated_recovery:
        cases = ['installed-activated-recovery']
    elif ingest_installed_live:
        cases = ['installed-live-initial-successor']
    elif ingest_live_refusals:
        cases = ['live-initial-refusals']
    elif ingest_live_routes:
        cases = ['live-initial-routes']
    elif ingest_live_guards:
        cases = ['live-initial-guards']
    elif ingest_live:
        cases = ['live-initial-activation']
    elif installed_empty_retirement:
        cases = ['installed-empty-retirement']
    elif ingest_empty_preparer:
        cases = ['initial-empty-preparer']
    elif ingest_empty_locks:
        cases = ['initial-empty-locks']
    elif ingest_empty_retirement:
        cases = ['initial-empty-retirement']
    elif ingest_gap_faults:
        cases = ['prepared-gap-policy-delivery']
    elif ingest_successor_gap:
        cases = ['prepared-initial-gap-completion']
    elif ingest_successor_history:
        cases = ['prepared-initial-history']
    elif ingest_succession:
        cases = ['initial-successor-preparation']
    elif ingest_recovery:
        cases = ['positive-recovery-signed' if ingest_recovery_signed
            else 'positive-recovery-interruptions' if ingest_recovery_interruptions else 'positive-recovery']
    elif ingest_history:
        cases = ['history']
    elif ingest_windows:
        cases = ['window-config', 'window-policy', 'window-local']
    elif installed:
        cases = ['pass','zero-topology','expired','policy-archive','existing-directory','registered-only',
                 'window-local','window-config','window-policy']
    elif lease:
        cases = ['pass','expired','control-expired','closing','draining','policy-first',
                 'policy-second','delayed','wrong-registration','wrong-control','cancelled']
    elif guards:
        cases = ['pass']
    elif policy:
        cases = ['pass', 'invalid', 'invalid-retained', 'certify-withdrawn',
                 'policy-provenance', 'policy-limit', 'policy-floor', 'policy-ephemeral', 'policy-second']
    elif refusals:
        cases = ['invalid', 'invalid-retained', 'invalid-withdrawn', 'dispatch-refused']
    else:
        db = clone('populated', 'bb_scope_54d1a18_basic')
        before, after = upgrade(db, 'populated')
        require(before['producer_requests'], 'nonempty-upgrade-workload')
        report['cases'].append(dict(name='populated-upgrade', status='PASS', prior_rows_xmin_filenodes_unchanged=True))
        save()
        cases = ['pass','changed','omitted','legacy-adopt','outside-collection','outside-request',
                 'no-source','certify-withdrawn','old-source-schema','old-binding-schema']
    installed_basis = None
    for name in cases:
        if registration_faults:
            completed = clone(name, 'bb_source_template' if registration_http_faults else registration_basis)
            report['cases'].append(registration_runs[name](pg, client, output, completed, name, rows))
            save()
        elif commits:
            completed = clone(name, 'bb_source_template')
            report['cases'].append(run_commits(pg, client, output, completed, name, rows))
            save()
        elif interruptions or (session_faults and name == 'close-signal'):
            from interruptions import run as run_interruption
            completed = clone(name, 'bb_source_template')
            report['cases'].append(run_interruption(pg,client,output,completed,name,rows,
                                                   sessions=session_faults))
            save()
        elif ingest_activated_acquisition_limits:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_acquisition_limits import run_all as run_activated_acquisition_limits
            report['cases'].append(run_activated_acquisition_limits(pg, client, output, rows, clone))
            save()
        elif ingest_activated_acquisition_http:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_acquisition_http import run_all as run_activated_acquisition_http
            report['cases'].append(run_activated_acquisition_http(pg, client, output, rows, clone))
            save()
        elif ingest_activated_acquisition_guards:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_acquisition_guards import run_all as run_activated_acquisition_guards
            report['cases'].append(run_activated_acquisition_guards(pg, client, output, rows, clone))
            save()
        elif ingest_activated_acquisition_claims:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_acquisition_claims import run_all as run_activated_acquisition_claims
            report['cases'].append(run_activated_acquisition_claims(pg, client, output, rows, clone))
            save()
        elif ingest_activated_acquisition_faults:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_acquisition_faults import run_all as run_activated_acquisition_faults
            report['cases'].append(run_activated_acquisition_faults(pg, client, output, rows, clone))
            save()
        elif ingest_activated_acquisition:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_acquisition import run_all as run_activated_acquisition
            report['cases'].append(run_activated_acquisition(pg, client, output, rows, clone))
            save()
        elif ingest_activated_source_prefix:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_source_prefix import run_all as run_activated_source_prefix
            report['cases'].append(run_activated_source_prefix(pg, client, output, rows, clone))
            save()
        elif ingest_activated_source_locks:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_source_closure_locks import run as run_activated_source_locks
            report['cases'].append(run_activated_source_locks(pg, client, output, rows, clone))
            save()
        elif ingest_activated_source_durable:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_source_durable import run_all as run_activated_source_durable
            report['cases'].append(run_activated_source_durable(pg, client, output, rows, clone))
            save()
        elif ingest_activated_source_faults:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_source_closure_faults import run as run_activated_source_faults
            report['cases'].append(run_activated_source_faults(pg, client, output, rows, clone))
            save()
        elif ingest_activated_source:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_source_closure import run_all as run_activated_source
            report['cases'].append(run_activated_source(pg, client, output, rows, clone))
            save()
        elif ingest_activated_refusals:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_recovery_refusals import run_all as run_activated_refusals
            report['cases'].append(run_activated_refusals(pg, client, output, rows, clone))
            save()
        elif ingest_activated_zero:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_recovery_zero import run_all as run_activated_zero
            report['cases'].append(run_activated_zero(pg, client, output, rows, clone))
            save()
        elif ingest_activated_faults:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_recovery_faults import run_all as run_activated_faults
            report['cases'].append(run_activated_faults(pg, client, output, rows, clone))
            save()
        elif ingest_activated_recovery:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from activated_recovery import run_all as run_activated_recovery
            report['cases'].append(run_activated_recovery(pg, client, output, rows, clone))
            save()
        elif ingest_installed_live:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from installed_live import run_all as run_installed_live
            report['cases'].append(run_installed_live(pg, client, output, rows, clone))
            save()
        elif ingest_live_refusals:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_live_refusals import run as run_initial_live_refusals
            report['cases'].append(run_initial_live_refusals(pg, client, output, rows, clone))
            save()
        elif ingest_live_routes:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_live_routes import run as run_initial_live_routes
            report['cases'].append(run_initial_live_routes(pg, client, output, rows, clone))
            save()
        elif ingest_live_guards:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_live_guards import run as run_initial_live_guards
            report['cases'].append(run_initial_live_guards(pg, client, output, rows, clone))
            save()
        elif ingest_live:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_live import run as run_initial_live
            report['cases'].append(run_initial_live(pg, client, output, rows, clone))
            save()
        elif installed_empty_retirement:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from installed_empty import run as run_installed_empty
            report['cases'].append(run_installed_empty(pg, client, output, rows, clone))
            save()
        elif ingest_empty_preparer:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_empty_preparer import run as run_empty_preparer
            report['cases'].append(run_empty_preparer(pg, client, output, rows, clone))
            save()
        elif ingest_empty_locks:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_empty_locks import run as run_empty_locks
            report['cases'].append(run_empty_locks(pg, client, output, rows, clone))
            save()
        elif ingest_empty_retirement:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_empty import run as run_initial_empty
            report['cases'].append(run_initial_empty(pg, client, output, rows, clone, commits=ingest_empty_commits))
            save()
        elif ingest_gap_faults:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_gap_faults import run as run_gap_faults
            report['cases'].append(run_gap_faults(pg, client, output, rows, clone))
            save()
        elif ingest_successor_gap:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_gap import run as run_initial_gap
            report['cases'].append(run_initial_gap(pg, client, output, rows, clone))
            save()
        elif ingest_successor_history:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_history import run as run_initial_history
            report['cases'].append(run_initial_history(pg, client, output, rows, clone))
            save()
        elif ingest_succession:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from initial_succession import run as run_succession
            report['cases'].append(run_succession(pg, client, output, rows, clone, commits=ingest_succession_commits))
            save()
        elif ingest_recovery:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            if ingest_recovery_signed:
                from installed_signed_recovery import run as run_signed_recovery
                report['cases'].append(run_signed_recovery(pg,client,output,rows,clone,
                    close_source=ingest_source_closure,
                    schedules='retire-obligations' if ingest_retirement_obligations else 'retire-locks' if ingest_retirement_locks else 'retire-faults' if ingest_retirement_faults else 'retire' if ingest_retirement else 'abandon-faults' if ingest_abandonment_faults else 'abandon-limits' if ingest_abandonment_limits else 'abandon-guards' if ingest_abandonment_guards else 'abandon' if ingest_abandonment else 'locks' if ingest_source_locks else 'faults' if ingest_source_faults else None))
            else:
                from installed_recovery import run as run_recovery
                completed = clone(name, 'bb_source_template')
                report['cases'].append(run_recovery(pg,client,output,completed,name,rows,clone))
            save()
        elif ingest_history:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from historical import run as run_history
            completed = clone(name, 'bb_source_template')
            report['cases'].append(run_history(pg,client,output,completed,name,rows,clone))
            save()
        elif installed:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            if installed_archive_publication:
                from installed_archive_journey import run as run_installed
            elif installed_archive_setup:
                from installed_archive import run as run_installed
            elif installed_sync:
                from installed_sync import run as run_installed
            elif export_status:
                from control_status import run as run_installed
            elif job_gap and name.startswith('sync-'):
                from installed_sync import run as run_installed
            elif jobs_http:
                from initial_jobs import run as run_installed
            elif scope_http:
                from scopes import run as run_installed
            elif registration_http:
                from registrations import run as run_installed
            elif enrollment_http:
                from enrollments import run as run_installed
            elif sessions:
                from sessions import run as run_installed
            else:
                from installed import run as run_installed
            template_name = (registration_basis if name in ('registration-upgrade', 'scope-upgrade')
                else installed_basis if name == 'registered-only' else 'bb_source_template')
            completed = clone(name, template_name)
            archive_context = dict(resources=report, save=save, build=build, built=built) if installed_archive else {}
            if job_gap and name.startswith('sync-'):
                archive_context = dict(gap_schema=27 if name == 'sync-gap-uncorrected' else 28)
            report['cases'].append(run_installed(pg,client,output,completed,name,rows,**archive_context))
            if name == 'pass': installed_basis = completed
            save()
        elif lease:
            sys.path.insert(0, str(SOURCE / 'experiments/ingest-bootstrap'))
            from lease import run as run_lease
            completed = clone(name, 'bb_source_template')
            report['cases'].append(run_lease(pg, client, output, completed, name, rows))
            save()
        else:
            completed = run_case(name)
    if guards:
        from binding_guards import run_policy_race

        def owned():
            item = json.loads(lab.docker('inspect', target['Id']).stdout)[0]
            require(item['Config']['Labels'][lab.LABEL] == pg['owner'] and item['State']['Running'], 'race-pg-owner')

        basis_rows = rows(completed)
        for name in ['writer_commit', 'writer_rollback', 'scope_commit', 'scope_rollback', 'repeatable']:
            db = clone(name, completed)
            report['cases'].append(run_policy_race(pg, db, name, rows, output, owned))
            save()
        # Discriminate the scope epoch predicate specifically. Only this new
        # clone loses it; no source, applied migration or prior outcome changes.
        from concurrency_cases import Mismatch
        db = clone('negative_no_epoch', completed)
        definition = lab.sql(pg, db, "SELECT pg_get_functiondef('control.require_acquisition_scope(bytea,bytea,bytea,bytea)'::regprocedure)")
        predicate = 'AND s.policy_revision = d.policy_revision'
        require(definition.count(predicate) == 1, 'negative-control-definition')
        altered = definition.replace(predicate, 'AND true /* isolated negative control */')
        lab.sql(pg, db, altered)
        negative = output / 'negative_no_epoch'
        negative.mkdir()
        try:
            run_policy_race(pg, db, 'writer_commit', rows, negative, owned)
        except Mismatch as error:
            require(str(error) == 'unexpected-sqlstate', 'wrong-negative-failure')
        else:
            raise RuntimeError('epoch-negative-control-was-not-detected')
        result = json.loads((negative / 'writer_commit-race.json').read_text())
        require(result['status'] == 'FAIL' and not result['cleanup_errors']
            and len(result['barriers']) == 1
            and result['responses'][-1]['answer']['sqlstate'] == '00000', 'wrong-negative-outcome')
        report['negative_control'] = dict(database=db, status='EXPECTED_FAIL',
            original_definition_sha256=sha(definition.encode()), altered_definition_sha256=sha(altered.encode()),
            reason='Writer-first policy commit incorrectly permits original scope when its epoch predicate is removed')
        save()
        require(rows(completed) == basis_rows, 'signed-parent-changed')
    require(rows('bb_scope_54d1a18_basic') == basic and rows('bb_source_template') == template
        and (basis / 'result.json').read_bytes() == basis_report, 'retained-original-changed')
    if registration_http or registration_faults or scope_http:
        final_original = dict(database=registration_basis,
            rows=rows(registration_basis), physical=physical(registration_basis))
        lab.write_json(output / 'registration-basis-after.json', final_original)
        require(final_original == registration_original, 'retained-enrollment-original-changed')
    if installed_archive_publication:
        require({r['name'] for r in report['cases']} == set(ARCHIVE_PUBLICATION_CASES)
            and len(report['cases']) == len(ARCHIVE_PUBLICATION_CASES)
            and all(r['status'] == 'PASS' for r in report['cases']), 'installed-archive-publication-complete-case-set')
    if installed_archive_setup:
        require({r['name'] for r in report['cases']} == set(ARCHIVE_SETUP_CASES)
            and len(report['cases']) == len(ARCHIVE_SETUP_CASES)
            and all(r['status'] == 'PASS' for r in report['cases']), 'installed-archive-complete-case-set')
    if installed_sync:
        require({r['name'] for r in report['cases']} == set(selected_sync_cases)
            and len(report['cases']) == len(selected_sync_cases)
            and all(r['status'] == 'PASS' for r in report['cases']), 'installed-sync-complete-case-set')
    if ingest_activated_acquisition_limits:
        from activated_acquisition_limits import CASES as ACTIVATED_ACQUISITION_LIMIT_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-acquisition-limits'
            and report['cases'][0]['cli_invocations'] == 6
            and report['cases'][0]['finite_seed_invocations'] == 3
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_ACQUISITION_LIMIT_CASES)
            and [len(r['attempts']) for r in report['cases'][0]['cases']] == [2, 2, 1, 1]
            and ['seed' in r for r in report['cases'][0]['cases']] == [True, True, False, True]
            and all(r['status'] == 'PASS'
                and ('seed' not in r or r['seed']['oracle']['status'] == 'PASS')
                and all(a['status'] == 'PASS' for a in r['attempts'])
                for r in report['cases'][0]['cases']), 'installed-activated-acquisition-limit-case-set')
    if ingest_activated_acquisition_http:
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'activated-acquisition-http'
            and report['cases'][0]['cli_invocations'] == 1
            and len(report['cases'][0]['cases']) == 1
            and report['cases'][0]['cases'][0]['name'] == 'activated-acquisition-http'
            and report['cases'][0]['cases'][0]['status'] == 'PASS',
            'activated-acquisition-http-case-set')
    if ingest_activated_acquisition_guards:
        from activated_acquisition_guards import CASES as ACTIVATED_ACQUISITION_GUARD_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'activated-acquisition-sql-guards'
            and report['cases'][0]['sql_attempts'] == 14
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_ACQUISITION_GUARD_CASES)
            and report['cases'][0]['oracle']['attempts'] == 14
            and report['cases'][0]['oracle']['status'] == 'PASS'
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']),
            'activated-acquisition-sql-guard-case-set')
    if ingest_activated_acquisition_claims:
        from activated_acquisition_claims import CASES as ACTIVATED_ACQUISITION_CLAIM_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-acquisition-claims'
            and report['cases'][0]['cli_invocations'] == 2
            and report['cases'][0]['finite_seed_invocations'] == 1
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_ACQUISITION_CLAIM_CASES)
            and [len(r['attempts']) for r in report['cases'][0]['cases']] == [2]
            and all(r['status'] == 'PASS' and r['seed']['oracle']['status'] == 'PASS'
                and all(a['status'] == 'PASS' for a in r['attempts'])
                for r in report['cases'][0]['cases']), 'installed-activated-acquisition-claim-case-set')
    if ingest_activated_acquisition_faults:
        from activated_acquisition_faults import CASES as ACTIVATED_ACQUISITION_FAULT_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-acquisition-faults'
            and report['cases'][0]['cli_invocations'] == 4
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_ACQUISITION_FAULT_CASES)
            and [len(r['attempts']) for r in report['cases'][0]['cases']] == [2, 2]
            and all(r['status'] == 'PASS' and all(a['status'] == 'PASS' for a in r['attempts'])
                for r in report['cases'][0]['cases']), 'installed-activated-acquisition-fault-case-set')
    if ingest_activated_acquisition:
        from activated_acquisition import CASES as ACTIVATED_ACQUISITION_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-acquisition-abandonment'
            and report['cases'][0]['cli_invocations'] == 4
            and report['cases'][0]['finite_seed_invocations'] == 0
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_ACQUISITION_CASES)
            and [len(r['attempts']) for r in report['cases'][0]['cases']] == [2, 2]
            and all(r['status'] == 'PASS' and all(a['status'] == 'PASS' for a in r['attempts'])
                for r in report['cases'][0]['cases']), 'installed-activated-acquisition-case-set')
    if ingest_activated_source_prefix:
        from activated_source_prefix import CASES as ACTIVATED_SOURCE_PREFIX_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-source-prefix-refusals'
            and report['cases'][0]['cli_invocations'] == 3
            and report['cases'][0]['finite_seed_invocations'] == 1
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_SOURCE_PREFIX_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']),
            'installed-activated-source-prefix-refusal-case-set')
    if ingest_activated_source_locks:
        from activated_source_closure_locks import CASES as ACTIVATED_SOURCE_LOCK_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-source-closure-locks'
            and report['cases'][0]['cli_invocations'] == 2
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_SOURCE_LOCK_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']),
            'installed-activated-source-closure-lock-case-set')
    if ingest_activated_source_durable:
        from activated_source_durable import CASES as ACTIVATED_SOURCE_DURABLE_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-source-already-durable'
            and report['cases'][0]['cli_invocations'] == 2
            and report['cases'][0]['finite_seed_invocations'] == 1
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_SOURCE_DURABLE_CASES)
            and [len(r['attempts']) for r in report['cases'][0]['cases']] == [2]
            and all(r['status'] == 'PASS' and all(a['status'] == 'PASS' for a in r['attempts'])
                for r in report['cases'][0]['cases']), 'installed-activated-source-already-durable-case-set')
    if ingest_activated_source_faults:
        from activated_source_closure_faults import CASES as ACTIVATED_SOURCE_FAULT_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-source-closure-faults'
            and report['cases'][0]['cli_invocations'] == 4
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_SOURCE_FAULT_CASES)
            and [len(r['attempts']) for r in report['cases'][0]['cases']] == [2, 2]
            and all(r['status'] == 'PASS' and all(a['status'] == 'PASS' for a in r['attempts'])
                for r in report['cases'][0]['cases']), 'installed-activated-source-closure-fault-case-set')
    if ingest_activated_source:
        from activated_source_closure import CASES as ACTIVATED_SOURCE_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-source-closure'
            and report['cases'][0]['cli_invocations'] == 13
            and report['cases'][0]['finite_seed_invocations'] == 0
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_SOURCE_CASES)
            and [len(r['attempts']) for r in report['cases'][0]['cases']] == [3, 3, 4, 2, 1]
            and all(r['status'] == 'PASS' and all(a['status'] == 'PASS' for a in r['attempts'])
                for r in report['cases'][0]['cases']), 'installed-activated-source-closure-case-set')
    if ingest_activated_refusals:
        from activated_recovery_refusals import CASES as ACTIVATED_REFUSAL_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-recovery-refusals'
            and report['cases'][0]['cli_invocations'] == 9
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_REFUSAL_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']),
            'installed-activated-recovery-refusal-case-set')
    if ingest_activated_zero:
        from activated_recovery_zero import CASES as ACTIVATED_ZERO_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-recovery-source-zero'
            and report['cases'][0]['cli_invocations'] == 2
            and report['cases'][0]['finite_seed_invocations'] == 1
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_ZERO_CASES)
            and [len(r['attempts']) for r in report['cases'][0]['cases']] == [2]
            and all(r['status'] == 'PASS' and r['seed']['oracle']['status'] == 'PASS'
                and all(a['status'] == 'PASS' for a in r['attempts'])
                for r in report['cases'][0]['cases']), 'installed-activated-recovery-source-zero-case-set')
    if ingest_activated_faults:
        from activated_recovery_faults import CASES as ACTIVATED_FAULT_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-recovery-faults'
            and report['cases'][0]['cli_invocations'] == 8
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_FAULT_CASES)
            and [len(r['attempts']) for r in report['cases'][0]['cases']] == [2, 2, 2, 2]
            and all(r['status'] == 'PASS' and all(a['status'] == 'PASS' for a in r['attempts'])
                for r in report['cases'][0]['cases']), 'installed-activated-recovery-fault-case-set')
    if ingest_activated_recovery:
        from activated_recovery import CASES as ACTIVATED_RECOVERY_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-activated-recovery'
            and report['cases'][0]['cli_invocations'] == 8
            and [r['name'] for r in report['cases'][0]['cases']] == list(ACTIVATED_RECOVERY_CASES)
            and [len(r['attempts']) for r in report['cases'][0]['cases']] == [3, 3, 2]
            and all(r['status'] == 'PASS' and all(a['status'] == 'PASS' for a in r['attempts'])
                for r in report['cases'][0]['cases']), 'installed-activated-recovery-case-set')
    if ingest_installed_live:
        from installed_live import CASES as INSTALLED_LIVE_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-live-initial-successor'
            and report['cases'][0]['cli_invocations'] == 5
            and [r['name'] for r in report['cases'][0]['cases']] == list(INSTALLED_LIVE_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']),
            'installed-live-initial-successor-case-set')
    if ingest_live_refusals:
        from initial_live_refusals import CASES as LIVE_REFUSAL_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'live-initial-refusals'
            and [r['name'] for r in report['cases'][0]['cases']] == list(LIVE_REFUSAL_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']),
            'live-initial-refusals-case-set')
    if ingest_live_routes:
        from initial_live_routes import CASES as LIVE_ROUTE_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'live-initial-routes'
            and [r['name'] for r in report['cases'][0]['cases']] == list(LIVE_ROUTE_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']),
            'live-initial-routes-case-set')
    if ingest_live_guards:
        from initial_live_guards import CASES as LIVE_GUARD_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'live-initial-guards'
            and [r['name'] for r in report['cases'][0]['cases']] == list(LIVE_GUARD_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']),
            'live-initial-guards-case-set')
    if ingest_live:
        from initial_live import CASES as LIVE_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'live-initial-activation'
            and [r['name'] for r in report['cases'][0]['cases']] == list(LIVE_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']),
            'live-initial-activation-case-set')
    if installed_empty_retirement:
        from installed_empty import CASES as INSTALLED_EMPTY_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'installed-empty-retirement'
            and report['cases'][0]['cli_invocations'] == 11
            and [r['name'] for r in report['cases'][0]['cases']] == list(INSTALLED_EMPTY_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']),
            'installed-empty-retirement-case-set')
    if ingest_empty_preparer:
        from initial_empty_preparer import CASES as EMPTY_PREPARER_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and report['cases'][0]['name'] == 'initial-empty-preparer'
            and [r['name'] for r in report['cases'][0]['cases']] == list(EMPTY_PREPARER_CASES)
            and all(r['status'] == 'PASS' and r['oracle']['processes'] == 2
                for r in report['cases'][0]['cases']), 'initial-empty-preparer-case-set')
    if ingest_empty_locks:
        from initial_empty_locks import CASES as EMPTY_LOCK_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and [r['name'] for r in report['cases'][0]['cases']] == list(EMPTY_LOCK_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']), 'initial-empty-lock-case-set')
    if ingest_empty_retirement:
        from initial_empty import CASES as EMPTY_CASES, COMMIT_CASES as EMPTY_COMMIT_CASES
        selected_empty = EMPTY_COMMIT_CASES if ingest_empty_commits else EMPTY_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and [r['name'] for r in report['cases'][0]['cases']] == list(selected_empty)
            and all(r['status'] == 'PASS' and (not ingest_empty_commits or len(r['attempts']) == 2)
                for r in report['cases'][0]['cases']), 'initial-empty-retirement-case-set')
    if ingest_gap_faults:
        from initial_gap_faults import CASES as GAP_FAULT_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and [r['name'] for r in report['cases'][0]['cases']] == list(GAP_FAULT_CASES)
            and all(r['status'] == 'PASS' and len(r['attempts']) == 2
                for r in report['cases'][0]['cases']), 'prepared-gap-policy-delivery-case-set')
    if ingest_successor_gap:
        from initial_gap import CASES as GAP_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and [r['name'] for r in report['cases'][0]['cases']] == list(GAP_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']), 'prepared-initial-gap-case-set')
    if ingest_successor_history:
        from initial_history import CASES as HISTORY_CASES
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and [r['name'] for r in report['cases'][0]['cases']] == list(HISTORY_CASES)
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']), 'prepared-initial-history-case-set')
    if ingest_succession:
        expected = (['successor-commit-before', 'successor-commit-after'] if ingest_succession_commits else
            ['successor-normal', 'successor-gap', 'successor-old-schema', 'successor-rollback'])
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and [r['name'] for r in report['cases'][0]['cases']] == expected
            and all(r['status'] == 'PASS' for r in report['cases'][0]['cases']), 'initial-successor-case-set')
    if ingest_recovery:
        expected = 3 if ingest_retirement_obligations else 4 if ingest_retirement_locks or ingest_retirement_faults else 10 if ingest_retirement else 4 if ingest_abandonment_faults else 6 if ingest_abandonment_limits else 11 if ingest_abandonment_guards else 6 if ingest_abandonment else 2 if ingest_source_locks else 4 if ingest_source_closure else 3 if ingest_recovery_interruptions or ingest_recovery_signed else 9
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and len(report['cases'][0]['cases']) == expected, 'positive-recovery-case-set')
    if ingest_history:
        require(len(report['cases']) == 1 and report['cases'][0]['status'] == 'PASS'
            and len(report['cases'][0]['cases']) == 10, 'historical-lookup-case-set')
    if ingest_windows:
        require([r['name'] for r in report['cases']] == cases
            and all(r['status'] == 'PASS' for r in report['cases']), 'installed-ingest-window-case-set')
    report['status'] = success_status
except BaseException as error:
    report.update(status='FAIL', failure=type(error).__name__ + ': ' + str(error))
    raise
finally:
    errors = []
    owned = [target['Id']] + ([report['client']['id']] if report['client'] else [])
    for ident in reversed(owned):
        try:
            item = json.loads(lab.docker('inspect', ident).stdout)[0]
            key, wanted = (lab.LABEL, pg['owner']) if ident == target['Id'] else (label, owner)
            require(item['Config']['Labels'][key] == wanted, 'cleanup-owner')
            if item['State']['Running']:
                lab.docker('stop', '--time', '10', ident)
        except BaseException as error:
            errors.append('stop-' + type(error).__name__)
    try:
        current = {c['Id']: projection(c) for c in containers()}
    except BaseException as error:
        current = {}
        errors.append('inventory-' + type(error).__name__)
    report.update(cleanup_errors=errors, missing_owned=[i for i in owned if i not in current],
        terminal=[current[i] for i in owned if i in current],
        unrelated_changed=[c['Id'] for c in prior if c['Id'] != target['Id'] and current.get(c['Id']) != projection(c)],
        finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), free_bytes_after=shutil.disk_usage(root).free)
    if errors or report['missing_owned'] or report['unrelated_changed'] or any(
        c['State']['Running'] or c['State']['Pid'] or c['State']['OOMKilled'] for c in report['terminal']):
        report['status'] = 'INCOMPLETE_LAB_AUDIT'
    save()
require(report['status'] == success_status, 'incomplete-binding')
print(json.dumps(dict(status=report['status'], cases=len(report['cases']))))
