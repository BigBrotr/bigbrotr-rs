"""Actual application Index build/tests; no engine, installed or release claim."""
import json
from pathlib import Path
import re
import shutil
import time

from tools.build import file_hash
from tools.ci import execute

STATUS = 'PASS_INITIAL_INDEX_APPLICATION_COMPONENTS'
GROUPS = {
    'clickhouse': 'storage::clickhouse::',
    'indexer': 'storage::indexer::',
    'index-publication': 'storage::postgres::control_owner::index_publication::',
    'postgres-route': 'storage::postgres::transport::route_tests::',
    'archive-driver-unpolled': 'storage::archive::lifetime::tests::catalog_transport_retains_custody_even_before_its_future_is_polled',
    'archive-driver-drop': 'storage::archive::lifetime::tests::cancelled_transport_is_destroyed_before_its_custody_descriptor',
    'archive-inputs': 'storage::archive::inputs::tests::',
    'archive-input-custody': 'storage::archive::initialization::tests::inputs::',
    'archive-expectations': 'storage::postgres::control_owner::archive_retained_frontier::',
    'source-head': 'storage::rocks::store::source_head_tests::',
    'source-closure': 'storage::rocks::store::admission_tests::source_closure_tests::',
    'lineage-pending': 'storage::rocks::store::admission_tests::initial_reconstruction_tests::lineage_tests::lineage_replay_tests::lineage_pending_tests::',
    'signed-source-head': 'storage::rocks::store::admission_tests::actual_signed_event_commits_payload_scope_and_receive_with_dictionary_and_receipt',
    'role-observation': 'management::roles::tests::',
    'observation-clock': 'runtime::policy_authority::authority_tick_elapsed_preserves_precision_and_refuses_regression',
}
SOURCE_REQUIRED = {
    'source-closure': tuple(GROUPS['source-closure'] + name for name in (
        'initial_historical_batch_reader_requires_complete_proof_and_preserves_signed_bytes',
        'source_closure_requires_every_original_request_and_preserves_all_native_bytes',
        'source_closure_preserves_zero_and_refuses_unissued_reservations',
        'source_closure_refuses_native_counter_disagreement_and_another_producer',
    )),
    'lineage-pending': tuple(GROUPS['lineage-pending'] + name for name in (
        'lineage_pending_reads_original_historical_and_current_batches_without_writes',
        'lineage_pending_requires_full_head_without_pending_preparation',
        'lineage_pending_refuses_foreign_zero_out_of_cut_and_interior_identities',
        'lineage_pending_preserves_limit_cancel_and_grant_errors_without_delivery',
        'lineage_pending_refuses_stale_native_and_physical_input_custody',
    )),
}
# A broad filter must not pass when a new owning module was never wired in.
REQUIRED_MODULES = (
    'storage::clickhouse::adapter::operation::tests::',
    'storage::clickhouse::adapter::readback::tests::',
    'storage::clickhouse::adapter::recovery::tests::',
    'storage::clickhouse::schema::tests::',
    'storage::clickhouse::lane::tests::',
    'storage::clickhouse::rows::tests::',
    'storage::clickhouse::transport::tests::',
    'storage::clickhouse::transport::metadata_tests::',
    'storage::indexer::plan::tests::',
    'storage::indexer::custody::tests::',
    'storage::indexer::state::tests::',
    'storage::postgres::control_owner::index_publication::handoff::tests::',
    'storage::rocks::store::source_head_tests::',
    'management::roles::tests::',
    'storage::archive::inputs::tests::',
    'storage::archive::initialization::tests::inputs::owned_reader::',
    'storage::postgres::control_owner::archive_retained_frontier::read_expectations::tests::',
)

# Closed initial service scope, derived from the owning source rather than an
# observed successful listing. Library test inventory cannot stand in for these
# binary-only modules. Names/counts change only with a reviewed scope change.
INITIAL = 'interfaces::serve::initial_ingest::'
BINARY_GROUPS = {
    'initial-index': INITIAL + 'index::',
    'initial-commands': INITIAL + 'commands::tests::',
    'initial-sync-diagnostic': INITIAL + 'sync_diagnostic::tests::',
    'initial-supervision': INITIAL + 'supervision::tests::',
    'initial-watchdog': INITIAL + 'tests::',
    'initial-cli': 'interfaces::serve::tests::initial_ingest_startup_requires_an_explicit_flag',
    'index-cli': 'interfaces::serve::tests::index_initialization_requires_explicit_fresh_ingest_and_refuses_successors',
    'initial-readiness': INITIAL + 'readiness::tests::',
    'role-status-http': 'interfaces::management::tests::roles::',
    'ordinary-observations': 'interfaces::serve::role_status::tests::',
    'initial-role-route': 'interfaces::management::tests::profiles::initial_profile_preserves_sessions_and_refuses_other_unwired_operations',
}
BINARY_EXACT = ('initial-cli', 'index-cli', 'initial-role-route')
BINARY_REQUIRED = {
    'initial-index': tuple(INITIAL + 'index::' + name for name in (
        'tests::accepted_profile_preserves_provenance_and_both_documents_actual_ceilings',
        'tests::source_validation_refuses_foreign_successor_policy_and_allocation_identity',
        'tests::initial_cut_has_no_publication_and_advances_only_the_same_complete_stream',
        'tests::all_ten_family_outcomes_obey_counts_zero_and_checked_wire_ceiling_before_prepare',
        'tests::reservations_distinguish_unavailable_from_impossible_and_do_not_wait',
        'tests::cancellation_and_expiry_refuse_before_dispatch',
        'tests::startup_capacity_includes_retained_control_inputs_and_index_preparation',
        'prepare::tests::ordinary_preparation_never_opens_initializer_or_creates_state_and_retains_credit',
        'prepare::tests::explicit_initialization_requires_the_separate_protected_pair',
        'prepare::tests::reservation_and_tls_profile_refusals_precede_dns_and_secret_reads',
        'prepare::tests::every_dns_answer_must_match_the_fixed_boundary_and_answer_limit',
        'prepare::tests::numeric_mapped_loopback_needs_no_resolver_and_unsafe_endpoint_forms_refuse',
        'prepare::tests::root_and_credential_refusals_preserve_input_and_return_the_grant',
        'prepare::tests::engine_envelope_preserves_independent_control_credit_and_never_waits',
        'readiness::tests::readiness_requires_exact_current_initial_source_cut_not_idle_or_a_partial_vector',
        'readiness::tests::readiness_capture_refuses_owner_changes_pending_and_inconsistent_published_heads',
        'readiness::tests::readiness_uses_oldest_read_and_a_strict_whole_sweep_window',
        'readiness::tests::readiness_abandon_does_not_reset_attempt_cooldown_or_erase_a_failure',
        'readiness::tests::readiness_empty_failed_cancelled_and_parked_caches_never_invent_a_verified_cut',
        'catch_up::tests::recovery_published_cursor_excludes_retained_pending_and_unplanned_backlog',
        'catch_up::tests::recovery_published_cursor_refuses_missing_changed_and_foreign_heads',
        'catch_up::tests::recovery_cursor_advances_only_for_the_exact_new_publication_receipt',
        'catch_up::tests::finite_recovery_accepts_only_original_or_exact_initial_successor_metadata',
        'catch_up::tests::explicit_handoff_head_retains_old_cut_and_zero_successor_without_a_batch',
        'catch_up::tests::finite_history_counts_global_requests_and_one_separate_bridge',
        'catch_up::tests::complete_successor_vector_cannot_erase_or_advance_closed_predecessor',
        'catch_up::tests::recovery_capacity_includes_real_holders_pending_replay_and_physical_proof',
    )),
    'initial-commands': tuple(BINARY_GROUPS['initial-commands'] + name for name in (
        'sync_safe_point_retains_prepared_input_and_grant_until_reauthorization',
        'sync_wire_phase_requires_renewal_even_with_remaining_time',
        'sync_safe_point_refuses_failed_revalidation_and_cancellation',
        'renewal_admission_does_not_wait_for_a_timer_tick',
        'admission_is_rechecked_after_a_select_wait',
        'a_short_archive_horizon_defers_instead_of_borrowing_new_budget',
        'slow_valid_renewals_cannot_create_an_unbounded_renewal_only_loop',
        'external_deadline_and_cancellation_precede_first_nested_poll',
        'nested_phase_publication_keeps_the_absolute_archive_ceiling',
        'a_selected_http_request_retains_its_permit_until_consumed_or_dropped',
    )),
    'initial-sync-diagnostic': tuple(BINARY_GROUPS['initial-sync-diagnostic'] + name for name in (
        'an_expired_operation_never_polls_a_new_effect',
        'an_in_flight_admission_keeps_its_deadline_and_unknown_outcome',
        'renewal_does_not_relabel_prior_progress_as_a_new_operation',
        'pending_admission_and_following_certification_are_distinct',
        'dropping_a_pending_stage_never_marks_it_returned',
        'silent_updates_preserve_deadlines_and_do_not_notify',
        'returned_error_is_distinct_from_pending',
        'completed_dispatch_does_not_leak_into_later_watchdogs',
    )),
    'initial-supervision': tuple(BINARY_GROUPS['initial-supervision'] + name for name in (
        'parent_watchdog_freezes_sync_stage_before_grace',
        'a_stopped_preparer_returns_owned_state_only_after_join',
        'published_phase_wins_an_expired_cached_deadline',
        'stop_collects_the_blocking_worker_after_its_cleanup',
        'grace_expiry_never_reports_or_aborts_a_blocked_worker',
        'stop_revokes_a_listener_published_after_the_supervisor_snapshot',
    )),
    'initial-watchdog': tuple(BINARY_GROUPS['initial-watchdog'] + name for name in (
        'a_late_native_completion_cannot_reset_the_phase_watchdog',
        'a_phase_refusal_retains_the_prior_static_callsite',
    )),
    'initial-readiness': tuple(BINARY_GROUPS['initial-readiness'] + name for name in (
        'missing_native_observation_never_implies_empty_or_ready_source',
        'publication_requires_source_fresh_physical_rows_and_matching_cut',
    )),
    'role-status-http': tuple(BINARY_GROUPS['role-status-http'] + name for name in (
        'role_status_requires_session_csrf_and_closed_get_before_queueing',
        'role_status_delivers_only_redacted_observation_and_releases_body_grant',
        'role_status_bound_action_expiry_refuses_bytes_after_queue_completion',
    )),
    'ordinary-observations': tuple(BINARY_GROUPS['ordinary-observations'] + name for name in (
        'ordinary_status_does_not_infer_readiness_from_enabled_or_absent_owners',
        'ordinary_last_success_requires_terminal_cleanup_and_never_claims_full_readiness',
        'ordinary_observation_freshness_uses_oldest_required_evidence_at_thirty_seconds',
        'ordinary_cache_checks_aggregate_before_response_clone_and_retains_its_grant',
        'ordinary_cache_refuses_oversize_foreign_or_malformed_cut_without_erasing_last_success',
        'ordinary_ready_requires_both_joined_full_proof_and_relevant_completed_operation',
        'ordinary_full_proof_matches_every_selection_coordinate_not_an_old_pinned_export',
        'ordinary_changed_current_selection_cannot_revive_an_older_full_proof',
        'ordinary_invalid_current_metadata_refuses_without_reviving_an_inflight_old_candidate',
    )),
    **{name: (BINARY_GROUPS[name],) for name in BINARY_EXACT},
}


def listed_tests(text):
    names = re.findall(r'^([\w:]+): test$', text, re.M)
    summaries = re.findall(r'^(\d+) tests?, (\d+) benchmarks?$', text, re.M)
    if (len(summaries) != 1 or summaries[0] != (str(len(names)), '0')
            or not names or len(set(names)) != len(names)):
        raise ValueError('invalid compiled test inventory')
    return sorted(names)


def selected_tests(names, *, groups=GROUPS, required_modules=REQUIRED_MODULES,
                   required_tests=None):
    selected = {name: [test for test in names if test.startswith(prefix)]
                for name, prefix in groups.items()}
    if (not all(selected.values())
            or any(not any(test.startswith(prefix) for test in names)
                   for prefix in required_modules)):
        raise ValueError('required Index test module is absent from actual application')
    if required_tests is not None and (
            selected.keys() != required_tests.keys()
            or any(sorted(selected[name]) != sorted(expected)
                   for name, expected in required_tests.items())):
        raise ValueError('compiled binary tests differ from the required initial service scope')
    return selected


def completed_tests(text, expected):
    passed = re.findall(r'^test ([\w:]+) \.\.\. ok$', text, re.M)
    summaries = re.findall(
        r'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; '
        r'(\d+) measured; (\d+) filtered out; finished in .+$', text, re.M)
    if (len(summaries) != 1 or summaries[0][:4] != (str(len(expected)), '0', '0', '0')
            or sorted(passed) != sorted(expected) or not expected):
        raise ValueError('actual executed tests differ from the compiled inventory')


def retain_executable(log, kind, test, destination, target):
    candidates = []
    for line in log.read_text().splitlines():
        if not line.startswith('{'):
            continue
        item = json.loads(line)
        if (item.get('reason') == 'compiler-artifact' and item.get('executable')
                and item['target']['name'] == 'bigbrotr'
                and item['target']['kind'] == [kind] and item['profile']['test'] is test):
            candidates.append(Path(item['executable']))
    if len(candidates) != 1:
        raise ValueError('actual application executable is absent or ambiguous')
    executable = candidates[0]
    if (not executable.is_relative_to(target) or executable.is_symlink()
            or not executable.resolve(strict=True).is_relative_to(target.resolve(strict=True))
            or not executable.is_file() or destination.exists()):
        raise ValueError('invalid retained executable boundary')
    digest = file_hash(executable)
    shutil.copy2(executable, destination)
    if file_hash(destination) != digest:
        raise ValueError('retained executable differs from compiler output')
    return digest


def build_commands():
    common = ['--locked', '--offline', '--release', '-p', 'bigbrotr']
    return [
        ('compile', ['cargo', 'build', *common, '--lib', '--bin', 'bigbrotr', '--message-format=json']),
        ('compile-tests', ['cargo', 'test', *common, '--lib', '--bin', 'bigbrotr', '--no-run', '--message-format=json']),
        ('clippy', ['cargo', 'clippy', *common, '--lib', '--bin', 'bigbrotr', '--', '-D', 'warnings']),
    ]


def test_commands(artifacts):
    return [
        (name, [str(artifacts / 'bigbrotr-lib-tests'), prefix, '--test-threads=1'])
        for name, prefix in GROUPS.items()
    ] + [
        (name, [str(artifacts / 'bigbrotr-bin-tests'), prefix,
                *(['--exact'] if name in BINARY_EXACT else []), '--test-threads=1'])
        for name, prefix in BINARY_GROUPS.items()
    ]


def run(source, build, env, identity, sources):
    original = sources()
    identity = dict(identity, scope='Actual bigbrotr library and CLI build, focused library Index/PG route and binary initial-service unit tests and strict Clippy; no stubs, engine transactions, installed service, release or capacity qualification',
                    profile='initial-index', archive_worker='not built or bound')
    output = build.parent
    qualification = output / 'qualification.json'
    report = dict(identity, status='RUNNING', not_run=[
        'ClickHouse engine/schema/settings/insert/readback and PostgreSQL transactions',
        'Archive worker build or binding', 'Installed service/recovery/outage and release',
        'Full workspace/integration tests and capacity'])
    deadline = time.monotonic() + 1800

    def remaining(maximum):
        return max(0, min(maximum, deadline - time.monotonic()))

    def record():
        qualification.write_text(json.dumps(report, indent=2) + '\n')

    record()
    try:
        checks = output / 'build-checks'
        if execute(build_commands(), checks, source, env, identity, remaining(1800))['status'] != 'PASS':
            raise ValueError('actual application build or Clippy failed')
        artifacts = output / 'artifacts'
        artifacts.mkdir()
        executables = {
            name: retain_executable(checks / log, kind, test, artifacts / name, build / 'target')
            for name, log, kind, test in [
                ('bigbrotr', 'compile.log', 'bin', False),
                ('bigbrotr-lib-tests', 'compile-tests.log', 'lib', True),
                ('bigbrotr-bin-tests', 'compile-tests.log', 'bin', True),
            ]
        }
        identity['executables_sha256'] = executables
        report.update(identity)
        record()
        inventory = output / 'test-inventory'
        if execute([
            ('list', [str(artifacts / 'bigbrotr-lib-tests'), '--list', '--format', 'pretty']),
            ('list-bin', [str(artifacts / 'bigbrotr-bin-tests'), '--list', '--format', 'pretty']),
            ('build-info', [str(artifacts / 'bigbrotr'), 'build-info']),
        ], inventory, source, env, identity, remaining(60))['status'] != 'PASS':
            raise ValueError('application identity or test listing failed')
        info = json.loads((inventory / 'build-info.log').read_text())
        if (info['source'] != dict(state='builder-supplied', revision=identity['source'],
                                  tree=identity['tree'], dirty=False,
                                  manifest_sha256=identity['source_manifest_sha256'])
                or info['target'] != 'x86_64-unknown-linux-gnu'
                or info['profile_assertion'] != 'release' or info['archive_worker'] is not None
                or info['inputs_sha256'] != {name: original[name] for name in (
                    'Cargo.lock', 'rust-toolchain.toml', 'contracts/release/dependencies.json',
                    'contracts/protocol/versions.json', 'contracts/protocol/nip01-event.json')}):
            raise ValueError('application build identity mismatch')
        library_names = listed_tests((inventory / 'list.log').read_text())
        tests = selected_tests(library_names)
        selected_tests(library_names,
                       groups={name: GROUPS[name] for name in SOURCE_REQUIRED},
                       required_modules=(), required_tests=SOURCE_REQUIRED)
        binary_tests = selected_tests(
            listed_tests((inventory / 'list-bin.log').read_text()), groups=BINARY_GROUPS,
            required_modules=(), required_tests=BINARY_REQUIRED)
        tests.update(binary_tests)
        (inventory / 'selected.json').write_text(json.dumps(tests, indent=2) + '\n')
        identity['selected_tests'] = tests
        identity['test_executables'] = {
            **{name: 'bigbrotr-lib-tests' for name in GROUPS},
            **{name: 'bigbrotr-bin-tests' for name in BINARY_GROUPS},
        }
        report.update(identity)
        record()
        checks_run = output / 'tests'
        if execute(test_commands(artifacts), checks_run, source, env, identity, remaining(120))['status'] != 'PASS':
            raise ValueError('focused actual application tests failed')
        for name, expected in tests.items():
            completed_tests((checks_run / (name + '.log')).read_text(), expected)
        if sources() != original or any(file_hash(artifacts / name) != digest
                                        for name, digest in executables.items()):
            raise ValueError('source or retained executable changed during checks')
        if any(file_hash(Path(tool['path'])) != tool['sha256']
               for tool in identity['native_toolchain']['tools'].values()):
            raise ValueError('native tool changed during checks')
        report.update(identity, status=STATUS, build_info=info,
                      tests={name: len(names) for name, names in tests.items()},
                      evidence_sha256={str(p.relative_to(output)): file_hash(p)
                                       for directory in (checks, inventory, checks_run)
                                       for p in sorted(directory.iterdir()) if p.is_file()})
    except BaseException as error:
        report.update(status='FAIL', failure=type(error).__name__)
        raise
    finally:
        record()
