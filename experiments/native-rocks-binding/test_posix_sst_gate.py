#!/usr/bin/env python3
"""Synthetic journal controls only: no interposition, engine or POSIX fault run."""
import copy
import errno
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location(
    'posix_sst_gate', Path(__file__).with_name('posix_sst_gate.py'))
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)
PID = 100


def journal():
    """Literal witness shape; these records are synthetic, never run evidence."""
    common = dict(pid=PID, tid=200, invalid=0, fd=7, dev=99, ino=123,
                  flags=os.O_CREAT | os.O_TRUNC | os.O_WRONLY,
                  size=0, blocks=0, path='/posix/db/000005.sst',
                  operation='', count=0, offset=-1, returned=7, errno=0)
    result = []
    for event in ('ready', 'gate', 'released', 'io', 'end'):
        result.append(dict(common, event=event, seq=len(result)+1))
    result[3].update(operation='pwrite64', count=1563, offset=0,
                     returned=-1, errno=errno.ENOSPC)
    return result


class SyntheticJournalControls(unittest.TestCase):
    def reject(self, change):
        records = journal()
        change(records)
        with self.assertRaises(AssertionError):
            GATE.validate_native(records, PID)

    def test_complete_synthetic_witness_accepts_all_three_write_spellings(self):
        for operation, offset in (('write', -1), ('pwrite', 0), ('pwrite64', 0)):
            with self.subTest(operation=operation):
                records = journal()
                records[3].update(operation=operation, offset=offset)
                result = GATE.validate_native(records, PID)
                self.assertEqual(result['real_enospc_writes'], [records[3]])

    def test_successful_write_with_stale_errno_is_not_enospc(self):
        self.reject(lambda records: records[3].update(returned=1563))

    def test_different_io_error_is_not_nospace(self):
        self.reject(lambda records: records[3].update(errno=errno.EIO))

    def test_fallocate_failure_is_not_a_write_witness(self):
        self.reject(lambda records: records[3].update(operation='fallocate'))

    def test_missing_gate_release_write_or_end_is_not_pass(self):
        for removed in ('gate', 'released', 'io', 'end'):
            with self.subTest(removed=removed):
                def remove(records):
                    records[:] = [record for record in records if record['event'] != removed]
                    for seq, record in enumerate(records, 1):
                        record['seq'] = seq
                self.reject(remove)

    def test_duplicate_gate_or_sequence_is_not_pass(self):
        def duplicate(records):
            records.insert(2, copy.deepcopy(records[1]))
            for seq, record in enumerate(records, 1):
                record['seq'] = seq
        self.reject(duplicate)
        self.reject(lambda records: records[2].update(seq=2))

    def test_wrong_process_fd_inode_or_thread_cannot_supply_write(self):
        for key in ('pid', 'fd', 'dev', 'ino', 'tid'):
            with self.subTest(key=key):
                self.reject(lambda records: records[3].update({key: 999}))

    def test_nonempty_foreground_or_readonly_open_is_not_gate(self):
        for fields in (dict(size=1), dict(blocks=8), dict(tid=PID),
                       dict(flags=os.O_CREAT | os.O_TRUNC | os.O_RDONLY),
                       dict(flags=os.O_CREAT | os.O_WRONLY),
                       dict(path='/posix/db/LOG'), dict(returned=-1)):
            with self.subTest(fields=fields):
                self.reject(lambda records: records[1].update(fields))

    def test_release_must_preserve_gate_identity(self):
        self.reject(lambda records: records[2].update(ino=999))

    def test_write_before_release_is_not_pass(self):
        def reorder(records):
            records[2]['seq'], records[3]['seq'] = records[3]['seq'], records[2]['seq']
        self.reject(reorder)

    def test_protocol_timeout_or_lost_record_invalidates_other_witnesses(self):
        self.reject(lambda records: records[-1].update(invalid=1))
        self.reject(lambda records: records[2].update(event='gate_timeout_or_protocol_error'))
        self.reject(lambda records: records[-1].update(seq=70))


class OuterCustodyHold(BaseException):
    """Synthetic stop at a non-returning production custody boundary."""


class SyntheticCleanupControls(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='bigbrotr-gate-control-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)/'posix'
        self.evidence = Path(temporary.name)/'evidence'
        (self.root/'db').mkdir(parents=True)
        self.evidence.mkdir()
        self.originals = {'request1.original': b'literal-original-one',
                          'request2.original': b'literal-original-two'}
        for name, data in self.originals.items():
            (self.root/'db'/name).write_bytes(data)
        self.child = Mock(pid=PID, returncode=-9)
        self.child.poll.return_value = None
        self.child.wait.return_value = -9
        self.broken_journal = Mock(side_effect=OSError(errno.EIO, 'synthetic journal failure'))

    def assert_preserved(self):
        self.assertEqual({path.name: path.read_bytes()
                          for path in (self.evidence/'abnormal-db').iterdir()}, self.originals)
        self.child.wait.assert_called_once_with(timeout=5)

    def test_exit_between_poll_and_kill_still_joins_and_preserves_with_broken_journal(self):
        with patch.object(GATE.os, 'killpg', side_effect=ProcessLookupError):
            GATE.join_and_preserve(self.child, self.root, self.evidence,
                                   self.broken_journal, failed=True)
        self.assert_preserved()

    def test_partial_retained_manifest_cannot_suppress_failure_copy(self):
        (self.evidence/'retained-files.json').write_text('{partial')
        self.child.poll.return_value = -9
        GATE.join_and_preserve(self.child, self.root, self.evidence,
                               self.broken_journal, failed=True)
        self.assert_preserved()

    def test_signal_error_still_attempts_join_and_preserves(self):
        with patch.object(GATE.os, 'killpg', side_effect=PermissionError):
            GATE.join_and_preserve(self.child, self.root, self.evidence,
                                   self.broken_journal, failed=True)
        self.assert_preserved()

    def test_join_timeout_or_error_holds_for_outer_despite_broken_journal(self):
        for error in (subprocess.TimeoutExpired('synthetic-child', 5), OSError(errno.EIO, 'wait')):
            with self.subTest(error=error):
                self.child.wait.side_effect = error
                with patch.object(GATE.os, 'killpg'), \
                     patch.object(GATE.time, 'sleep', side_effect=OuterCustodyHold) as hold:
                    with self.assertRaises(OuterCustodyHold):
                        GATE.join_and_preserve(self.child, self.root, self.evidence,
                                               self.broken_journal, failed=True)
                hold.assert_called_once_with(1)
                self.assertFalse((self.evidence/'abnormal-db').exists())

    def test_copy_error_holds_for_outer_despite_broken_journal(self):
        self.child.poll.return_value = -9
        with patch.object(GATE.shutil, 'copytree', side_effect=OSError(errno.ENOSPC, 'copy')), \
             patch.object(GATE.time, 'sleep', side_effect=OuterCustodyHold) as hold:
            with self.assertRaises(OuterCustodyHold):
                GATE.join_and_preserve(self.child, self.root, self.evidence,
                                       self.broken_journal, failed=True)
        hold.assert_called_once_with(1)

    def test_unknown_child_state_holds_for_outer_despite_broken_journal(self):
        self.child.poll.side_effect = OSError(errno.EIO, 'poll')
        with patch.object(GATE.time, 'sleep', side_effect=OuterCustodyHold) as hold:
            with self.assertRaises(OuterCustodyHold):
                GATE.join_and_preserve(self.child, self.root, self.evidence,
                                       self.broken_journal, failed=True)
        hold.assert_called_once_with(1)
        self.child.wait.assert_not_called()


if __name__ == '__main__':
    unittest.main()
