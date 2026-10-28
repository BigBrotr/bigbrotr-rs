"""Shared query trace bounds, lifecycle refusals and failure-safe cleanup; no engines."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import xml.etree.ElementTree as ET

from cases import first_launch as base
from cases import first_launch_index_query_trace as case
from cases import first_launch_index_commit as commit
from cases import first_launch_retained as retained
from cases.first_launch_index import IndexRuntime
from cases.first_launch_archive_outage import ArchiveOutageRuntime
from cases.first_launch_index_restart import RestartIndexRuntime
from test_first_launch import ROOT, arguments

TRACE_SERVER='<clickhouse><max_server_memory_usage>3221225472</max_server_memory_usage></clickhouse>'
TRACE_USERS='<clickhouse><profiles><default><log_queries>0</log_queries></default></profiles></clickhouse>'
TRACE_PROVISION="CREATE USER initializer IDENTIFIED WITH sha256_password BY 'fake';\nCREATE USER writer IDENTIFIED WITH sha256_password BY 'fake' HOST LOCAL SETTINGS max_concurrent_queries_for_user=1 MIN 1 MAX 1;\nCREATE USER reader IDENTIFIED WITH sha256_password BY 'fake';\n"


class QueryTraceControls(unittest.TestCase):
    def test_shared_methods_keep_each_existing_super_owner(self):
        for cls in (commit.IndexCommitRuntime,retained.RetainedRuntime):
            runtime=object.__new__(cls)
            self.assertIs(super(case.QueryTraceRuntime,runtime).prepare_state.__func__,RestartIndexRuntime.prepare_state)
            self.assertIs(super(case.QueryTraceRuntime,runtime).bootstrap.__func__,
                ArchiveOutageRuntime.bootstrap if cls is retained.RetainedRuntime else IndexRuntime.bootstrap)
            self.assertIs(super(case.QueryTraceRuntime,runtime).stop_container.__func__,base.Runtime.stop_container)
            self.assertIs(cls.collect_query_lifecycle,case.QueryTraceRuntime.collect_query_lifecycle)
        self.assertIs(super(retained.RetainedRuntime,object.__new__(retained.RetainedRuntime)).prepare_state.__func__,
            case.QueryTraceRuntime.prepare_state)

    def test_shared_helper_is_bound_in_actual_source_manifest_and_mismatch_refuses(self):
        class TraceOwner(case.QueryTraceRuntime,base.Runtime):
            pass
        import candidate
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);runtime=TraceOwner(arguments(root),{})
            selected=dict(bundle_root=str(root/'bundle'),manifest=dict(artifact=dict(source={}),
                assembly=dict(revision='a'*40,tree='b'*40)),manifest_sha256='c'*64,bundle_sha256='d'*64)
            def helpers():
                runtime.signed_sha256=base.digest(ROOT/'tests/fixtures/ingest/import-event.jsonl')
            with patch.object(candidate,'extract_candidate',return_value=selected), \
                    patch.object(runtime,'load_helpers',side_effect=helpers):
                runtime.candidate()
            relative='tests/acceptance/cases/first_launch_index_query_trace.py'
            self.assertEqual(runtime.inputs['acceptance_files_sha256'][relative],base.digest(ROOT/relative))
            runtime.source=root/'substituted';path=runtime.source/relative;path.parent.mkdir(parents=True)
            path.write_text('different helper')
            with patch.object(base.Runtime,'candidate') as parent:
                with self.assertRaisesRegex(ValueError,'query trace acceptance source mismatch'):runtime.candidate()
                parent.assert_not_called()

    def test_query_trace_changes_only_closed_writer_logging_and_queue(self):
        server,provision=case.query_trace_profile(TRACE_SERVER,TRACE_USERS,TRACE_PROVISION)
        original=TRACE_PROVISION.splitlines();actual=provision.splitlines()
        self.assertEqual((actual[0],actual[2]),(original[0],original[2]))
        self.assertIn('max_concurrent_queries_for_user=1 MIN 1 MAX 1,',actual[1])
        self.assertIn('log_queries_cut_to_length=1',actual[1]);self.assertIn('log_query_settings=0',actual[1])
        root=ET.fromstring(server);self.assertEqual(root.findtext('max_server_memory_usage'),'3221225472')
        self.assertEqual({c.tag:c.text for c in root.find('query_log')},dict(database='system',table='query_log',
            flush_interval_milliseconds='7500',max_size_rows='1024',reserved_size_rows='64',
            buffer_size_rows_flush_threshold='512',flush_on_crash='false'))
        with self.assertRaises(ValueError):case.query_trace_profile(TRACE_SERVER,TRACE_USERS.replace('>0<','>1<'),TRACE_PROVISION)
        with self.assertRaises(ValueError):case.query_trace_profile(TRACE_SERVER,TRACE_USERS,TRACE_PROVISION.replace('MAX 1','MAX 2'))

    def test_query_lifecycle_is_bounded_named_evidence_without_exclusion(self):
        positive='bigbrotr-index-trace-'+'a'*32
        row=dict(query_id=positive,initial_query_id=positive,user='writer',initial_user='writer',event_us=10,start_us=10,
                 duration_ms=0,exception_code=0,is_initial_query=1)
        rows=[dict(row,event='QueryStart'),dict(row,event='QueryFinish',event_us=20)]
        summary=case.lifecycle_summary(2,rows,positive)
        self.assertEqual((summary['status'],summary['termination_or_exclusion']),('CAPTURED','NOT_PROVEN'))
        self.assertEqual(summary['all_query_coverage'],'NOT_PROVEN')
        self.assertFalse(summary['query_finish_is_process_list_removal'])
        query='bigbrotr-index-'+'b'*32+'-2-36'
        refusal=dict(row,query_id=query,initial_query_id=query,event='ExceptionBeforeStart',exception_code=202)
        self.assertEqual(case.lifecycle_summary(3,rows+[refusal],positive)['exceptions'][0]['exception_code'],202)
        unmatched=dict(refusal,event='QueryStart',exception_code=0)
        self.assertEqual(case.lifecycle_summary(3,rows+[unmatched],positive)['status'],'INCOMPLETE')
        self.assertEqual(case.lifecycle_summary(4,rows+[refusal,refusal],positive)['status'],'INCOMPLETE')
        for changed,count in ((rows,3),(rows,case.TRACE_ROWS+1),(rows[:1],1),
                ([dict(rows[0],user='initializer'),rows[1]],2),([dict(rows[0],query='not permitted'),rows[1]],2)):
            with self.subTest(count=count),self.assertRaises(ValueError):case.lifecycle_summary(count,changed,positive)

    def test_diagnostic_failure_or_interruption_still_stops_owned_clickhouse(self):
        for cls,error in ((cls,error) for cls in (commit.IndexCommitRuntime,retained.RetainedRuntime)
                          for error in (RuntimeError('capture'),KeyboardInterrupt())):
            with tempfile.TemporaryDirectory() as directory:
                runtime=cls(arguments(Path(directory)),{})
                runtime.trace_output=Path(directory)/'trace';runtime.trace_output.mkdir()
                original=dict(stage='signed_acquisition',error_type='TimeoutExpired')
                base.save(runtime.root/'failure.json',original)
                with patch.object(runtime,'collect_query_lifecycle',side_effect=error),\
                        patch.object(RestartIndexRuntime,'stop_container',return_value='joined') as stop:
                    self.assertEqual(runtime.stop_container(dict(service='clickhouse')),'joined')
                    stop.assert_called_once()
                self.assertEqual(base.load(runtime.root/'failure.json'),original)
                self.assertEqual(base.load(runtime.trace_output/'terminal-summary.json')['status'],'INCOMPLETE')
                self.assertEqual(len(runtime.resources['cleanup_errors']),1)
        with tempfile.TemporaryDirectory() as directory:
            runtime=commit.IndexCommitRuntime(arguments(Path(directory)),{})
            runtime.trace_output=Path(directory)/'trace';runtime.trace_output.mkdir()
            runtime.resources['containers']=[dict(service='app',status='running',pid=1)]
            with patch.object(runtime,'collect_query_lifecycle') as capture,\
                    patch.object(RestartIndexRuntime,'stop_container',return_value='joined') as stop:
                self.assertEqual(runtime.stop_container(dict(service='clickhouse')),'joined')
                capture.assert_not_called();stop.assert_called_once()
        with tempfile.TemporaryDirectory() as directory:
            runtime=commit.IndexCommitRuntime(arguments(Path(directory)),{})
            runtime.trace_output=Path(directory)/'trace';runtime.trace_output.mkdir()
            with patch.object(runtime,'collect_query_lifecycle',side_effect=RuntimeError('capture')),\
                    patch.object(case,'save',side_effect=OSError('diagnostic storage')),\
                    patch.object(RestartIndexRuntime,'stop_container',return_value='joined') as stop:
                with self.assertRaises(OSError):runtime.stop_container(dict(service='clickhouse'))
                stop.assert_called_once()

    def capture_runtime(self,root,cutoff=None,unpaired=False):
        space=patch.object(case.shutil,'disk_usage',return_value=SimpleNamespace(free=100*1024**3))
        space.start();self.addCleanup(space.stop)
        runtime=retained.RetainedRuntime(arguments(root),{})
        runtime.trace_output=root/'trace';runtime.trace_output.mkdir()
        runtime.trace_positive=positive='bigbrotr-index-trace-'+'a'*32
        runtime.ch_entry=dict(id='owned',service='clickhouse')
        runtime.inspect=Mock();runtime.record_resources=Mock()
        if cutoff is not None:runtime.trace_start_through_us=cutoff
        row=dict(query_id=positive,initial_query_id=positive,user='writer',initial_user='writer',event_us=10,start_us=10,
            duration_ms=0,exception_code=0,is_initial_query=1)
        rows=[dict(row,event='QueryStart'),dict(row,event='QueryFinish',event_us=200)]
        if unpaired:
            query='bigbrotr-index-'+'b'*32+'-2-36'
            rows.append(dict(row,query_id=query,initial_query_id=query,event='QueryStart'))
        stdout='\n'.join(json.dumps(v) for v in [dict(kind='count',rows=len(rows))]+
            [dict(row,kind='query') for row in rows])
        runtime.docker=Mock(return_value=SimpleNamespace(returncode=0,stdout=stdout,stderr=''))
        return runtime

    def test_count_and_rows_share_cutoff_keep_late_finish_and_original_caps(self):
        for cutoff in (None,100):
            with tempfile.TemporaryDirectory() as directory:
                runtime=self.capture_runtime(Path(directory),cutoff)
                runtime.collect_query_lifecycle('terminal')
                call=runtime.docker.call_args;sql=call.kwargs['data']
                selected=case.query_trace_selection(cutoff)
                self.assertEqual(sql.count('WHERE '+selected+' '),2)
                self.assertEqual('query_start_time_microseconds)<=100' in sql,cutoff is not None)
                self.assertEqual(call.kwargs['timeout'],30)
                for option in ('--max_result_rows=8193','--max_result_bytes=4194304',
                               '--max_execution_time=10','--receive_timeout=15','--result_overflow_mode=throw'):
                    self.assertIn(option,call.args)
                rows=base.load(runtime.trace_output/'terminal-rows.json')
                self.assertEqual(rows[-1]['event_us'],200)  # Started before cutoff, finished afterwards.
                summary=base.load(runtime.trace_output/'terminal-summary.json')
                self.assertEqual(summary['status'],'CAPTURED')
                self.assertEqual(summary.get('query_start_through_us'),cutoff)

    def test_cutoff_types_bounds_and_rows_after_cutoff_refuse(self):
        for cutoff in (False,True,0,-1,2**63,1.0,'1','0 OR 1=1'):
            with self.subTest(cutoff=cutoff),self.assertRaises(ValueError):case.query_trace_selection(cutoff)
        for cutoff in (1,2**63-1):self.assertIn(str(cutoff),case.query_trace_selection(cutoff))
        with tempfile.TemporaryDirectory() as directory:
            runtime=self.capture_runtime(Path(directory),5)
            with self.assertRaisesRegex(ValueError,'row starts after prefix cutoff'):
                runtime.collect_query_lifecycle('terminal')

    def test_incomplete_prefix_diagnostic_stays_explicit_without_new_acceptance_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime=self.capture_runtime(Path(directory),100,unpaired=True)
            with patch.object(base.Runtime,'stop_container',return_value='joined') as stop:
                self.assertEqual(runtime.stop_container(dict(service='clickhouse')),'joined')
                stop.assert_called_once()
            summary=base.load(runtime.trace_output/'terminal-summary.json')
            self.assertEqual((summary['status'],summary['scope']),('INCOMPLETE','prefix_started_queries'))
            self.assertEqual(summary['query_start_through_us'],100)
            self.assertTrue(summary['missing_or_duplicate_lifecycle'])
            self.assertEqual(summary['all_query_coverage'],summary['termination_or_exclusion'])
            self.assertEqual(summary['termination_or_exclusion'],'NOT_PROVEN')
            self.assertNotIn('PASS',json.dumps(summary))
            self.assertEqual(runtime.resources['cleanup_errors'],[])


if __name__=='__main__':unittest.main()
