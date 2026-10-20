"""Independent textual process traces; no subprocesses, native code or engines."""
import unittest

from memory_trace import TraceError, audit_trace


# Successful clone return can appear after the child's first exec entry.
# A worker thread is a task in process 200, not a separately spawned worker.
THREADED = '''100 1000000000.000001 execve("/usr/bin/python3", ["python3", "-B", "setup.py"], 0x123 /* 1 var */) = 0
100 1000000000.000002 clone3({flags=CLONE_VM|CLONE_VFORK, exit_signal=SIGCHLD}, 88) = -1 ENOSYS (Function not implemented)
100 1000000000.000003 clone(child_stack=NULL, flags=CLONE_VM|CLONE_VFORK|SIGCHLD <unfinished ...>
200 1000000000.000004 execve("/build/archive-worker/bigbrotr-archive-worker", ["worker", "--ipc=3"], 0x123 /* 1 var */ <unfinished ...>
100 1000000000.000005 <... clone resumed>) = 200
200 1000000000.000006 <... execve resumed>) = 0
200 1000000000.000007 clone3({flags=CLONE_VM|CLONE_FS|CLONE_FILES|CLONE_SIGHAND|CLONE_THREAD|CLONE_SYSVSEM|CLONE_SETTLS|CLONE_PARENT_SETTID|CLONE_CHILD_CLEARTID, exit_signal=0, stack=0x123, stack_size=0x1000}, 88) = 201
201 1000000000.000008 exit(0) = ?
201 1000000000.000009 +++ exited with 0 +++
100 1000000000.000010 wait4(200,  <unfinished ...>
200 1000000000.000011 exit_group(0) = ?
200 1000000000.000012 +++ exited with 0 +++
100 1000000000.000013 <... wait4 resumed>[{WIFEXITED(s) && WEXITSTATUS(s) == 0}], 0, NULL) = 200
100 1000000000.000014 --- SIGCHLD {si_signo=SIGCHLD, si_code=CLD_EXITED, si_pid=200, si_uid=1000, si_status=0, si_utime=0, si_stime=0} ---
100 1000000000.000015 exit_group(0) = ?
100 1000000000.000016 +++ exited with 0 +++
'''

# A sibling thread can reap a process created by another thread in the same
# parent process. This trace also exercises fork, vfork and waitid.
PARENT_THREAD = '''100 1000000000.000001 execve("/usr/bin/python3", ["python3"], 0x123 /* 1 var */) = 0
100 1000000000.000002 fork() = 200
200 1000000000.000003 execve("/build/bigbrotr", ["bigbrotr"], 0x123 /* 1 var */) = 0
200 1000000000.000004 clone(child_stack=0x123, flags=CLONE_VM|CLONE_SIGHAND|CLONE_THREAD|CLONE_CHILD_CLEARTID, child_tidptr=0x456) = 201
201 1000000000.000005 vfork( <unfinished ...>
300 1000000000.000006 execve("/build/archive-worker/bigbrotr-archive-worker", ["worker"], 0x123 /* 1 var */) = 0
201 1000000000.000007 <... vfork resumed>) = 300
200 1000000000.000008 waitid(P_PID, 300, {}, WNOHANG|WEXITED, NULL) = 0
300 1000000000.000009 exit_group(0) = ?
300 1000000000.000010 +++ exited with 0 +++
200 1000000000.000011 waitid(P_PID, 300, {si_signo=SIGCHLD, si_code=CLD_EXITED, si_pid=300, si_uid=1000, si_status=0, si_utime=0, si_stime=0}, WEXITED, NULL) = 0
201 1000000000.000012 exit(0) = ?
201 1000000000.000013 +++ exited with 0 +++
200 1000000000.000014 exit_group(0) = ?
200 1000000000.000015 +++ exited with 0 +++
100 1000000000.000016 wait4(200, [{WIFEXITED(s) && WEXITSTATUS(s) == 0}], 0, NULL) = 200
100 1000000000.000017 exit_group(0) = ?
100 1000000000.000018 +++ exited with 0 +++
'''


def replace(trace, old, new):
    assert trace.count(old) == 1
    return trace.replace(old, new)


class MemoryTraceTests(unittest.TestCase):
    def test_explicit_zero_worker_phase_still_requires_complete_trace(self):
        trace = '''100 1000000000.000001 execve("/usr/bin/python3", ["python3"], NULL) = 0
100 1000000000.000002 exit_group(0) = ?
100 1000000000.000003 +++ exited with 0 +++
'''
        result = audit_trace(trace, minimum_workers=0)
        self.assertEqual((result['minimum_workers'], result['worker_count']), (0, 0))
        with self.assertRaises(TraceError):
            audit_trace(trace)
        with self.assertRaises(TraceError):
            audit_trace(trace.rsplit('100 1000000000.000003', 1)[0], minimum_workers=0)
        for value in (-1, 2, False, '0'):
            with self.subTest(value=value), self.assertRaises(TraceError):
                audit_trace(trace, minimum_workers=value)

    def test_split_calls_and_worker_thread(self):
        result = audit_trace(THREADED)
        self.assertEqual((result['worker_count'], result['process_count'], result['thread_count']), (1, 2, 1))
        worker, = result['workers']
        self.assertEqual((worker['pid'], worker['parent_pid'], worker['thread_tids']), (200, 100, [201]))
        self.assertEqual((worker['birth']['line'], worker['birth']['end_line']), (3, 5))
        self.assertEqual((worker['exec']['line'], worker['exec']['end_line']), (4, 6))
        self.assertEqual((worker['terminal']['line'], worker['reap']['end_line']), (12, 13))
        self.assertNotIn('start_ticks', worker)

    def test_waitid_reaper_is_actual_parent_process(self):
        worker, = audit_trace(PARENT_THREAD)['workers']
        self.assertEqual((worker['pid'], worker['parent_pid'], worker['birth']['pid'], worker['reap']['pid']),
                         (300, 200, 201, 200))
        self.assertEqual([entry['pid'] for entry in worker['lineage']], [200, 201, 300])

    def test_successful_clone3_process(self):
        trace = replace(THREADED,
            'clone(child_stack=NULL, flags=CLONE_VM|CLONE_VFORK|SIGCHLD <unfinished ...>',
            'clone3({flags=CLONE_VM|CLONE_VFORK, exit_signal=SIGCHLD}, 88 <unfinished ...>')
        trace = replace(trace, '<... clone resumed>', '<... clone3 resumed>')
        self.assertEqual(audit_trace(trace)['workers'][0]['pid'], 200)

    def test_zero_minimum_does_not_relax_observed_worker_success(self):
        trace = replace(THREADED, '200 1000000000.000011 exit_group(0)', '200 1000000000.000011 exit_group(1)')
        trace = replace(trace, '200 1000000000.000012 +++ exited with 0', '200 1000000000.000012 +++ exited with 1')
        trace = replace(trace, 'WEXITSTATUS(s) == 0', 'WEXITSTATUS(s) == 1')
        for minimum in (0, 1):
            with self.subTest(minimum=minimum), self.assertRaisesRegex(TraceError, 'successful exit'):
                audit_trace(trace, minimum_workers=minimum)

    def test_second_worker_has_distinct_birth_exec_and_reap(self):
        tail = '''100 1000000000.000015 vfork() = 300
300 1000000000.000016 execve("/build/archive-worker/bigbrotr-archive-worker", ["worker"], 0x123 /* 1 var */) = 0
300 1000000000.000017 exit_group(0) = ?
300 1000000000.000018 +++ exited with 0 +++
100 1000000000.000019 wait4(-1, [{WIFEXITED(s) && WEXITSTATUS(s) == 0}], WNOHANG, NULL) = 300
100 1000000000.000020 exit_group(0) = ?
100 1000000000.000021 +++ exited with 0 +++
'''
        trace = THREADED[:THREADED.index('100 1000000000.000015')] + tail
        self.assertEqual([row['pid'] for row in audit_trace(trace)['workers']], [200, 300])
        with self.assertRaisesRegex(TraceError, 'successful exec'):
            audit_trace(trace.replace(tail.splitlines(keepends=True)[1], ''))

    def test_poll_and_interrupted_wait_do_not_reap(self):
        trace = replace(THREADED, '100 1000000000.000010 wait4(200,  <unfinished ...>',
                        '100 1000000000.000009 wait4(200, 0x123, WNOHANG, NULL) = 0\n'
                        '100 1000000000.000009 wait4(200, 0x123, 0, NULL) = -1 EINTR (Interrupted system call)\n'
                        '100 1000000000.000010 wait4(200,  <unfinished ...>')
        self.assertEqual(audit_trace(trace)['worker_count'], 1)

    def test_incomplete_or_ambiguous_trace_refuses(self):
        mutations = {
            'truncated-file': THREADED[:-1],
            'missing-root-terminal': THREADED.rsplit('100 1000000000.000016', 1)[0],
            'missing-thread-terminal': THREADED.replace('201 1000000000.000009 +++ exited with 0 +++\n', ''),
            'missing-worker-terminal': THREADED.replace('200 1000000000.000012 +++ exited with 0 +++\n', ''),
            'unknown-worker-birth': THREADED.replace(THREADED.splitlines(keepends=True)[2], '')
                                           .replace(THREADED.splitlines(keepends=True)[4], ''),
            'signal-is-not-reap': THREADED.replace(THREADED.splitlines(keepends=True)[9], '')
                                         .replace(THREADED.splitlines(keepends=True)[12], ''),
            'missing-birth': THREADED.replace('100 1000000000.000003 clone(child_stack=NULL, flags=CLONE_VM|CLONE_VFORK|SIGCHLD <unfinished ...>\n', ''),
            'missing-exec-resume': THREADED.replace('200 1000000000.000006 <... execve resumed>) = 0\n', ''),
            'wrong-resume': replace(THREADED, '<... clone resumed>', '<... fork resumed>'),
            'wrong-waited-child': replace(THREADED, ', 0, NULL) = 200', ', 0, NULL) = 999'),
            'wrong-reaper': replace(PARENT_THREAD, '200 1000000000.000011 waitid', '100 1000000000.000011 waitid'),
            'nonreaping-waitid': replace(PARENT_THREAD, '}, WEXITED, NULL)', '}, WEXITED|WNOWAIT, NULL)'),
            'exit-status-mismatch': replace(THREADED, 'WEXITSTATUS(s) == 0', 'WEXITSTATUS(s) == 1'),
            'exit-request-mismatch': replace(THREADED, '200 1000000000.000011 exit_group(0)',
                                              '200 1000000000.000011 exit_group(1)'),
            'worker-nonzero': THREADED.replace('200 1000000000.000012 +++ exited with 0 +++',
                                                '200 1000000000.000012 +++ exited with 1 +++')
                                       .replace('WEXITSTATUS(s) == 0', 'WEXITSTATUS(s) == 1'),
            'worker-signalled': replace(THREADED, '200 1000000000.000012 +++ exited with 0 +++',
                                        '200 1000000000.000012 +++ killed by SIGKILL +++'),
            'alternate-worker-path': THREADED.replace('/build/archive-worker/bigbrotr-archive-worker',
                                                      '/other/bigbrotr-archive-worker'),
            'truncated-exec-path': THREADED.replace('"/build/archive-worker/bigbrotr-archive-worker",',
                                                     '"/build/archive-worker/bigbrotr-arch"...,'),
            'nonleader-exec': replace(THREADED, '201 1000000000.000008 exit(0) = ?',
                                      '201 1000000000.000008 execve("/bin/true", ["true"], NULL) = 0'),
            'untraced-child': replace(THREADED, 'flags=CLONE_VM|CLONE_VFORK|SIGCHLD <',
                                      'flags=CLONE_VM|CLONE_VFORK|CLONE_UNTRACED|SIGCHLD <'),
            'alternate-parent': replace(THREADED, 'flags=CLONE_VM|CLONE_VFORK|SIGCHLD <',
                                        'flags=CLONE_VM|CLONE_VFORK|CLONE_PARENT|SIGCHLD <'),
            'unresolved-tail': THREADED + '100 1000000000.000017 wait4(1, <unfinished ...>\n',
            'diagnostic-line': THREADED + 'strace: Process 400 detached\n',
            'unknown-call': replace(THREADED, '201 1000000000.000008 exit(0) = ?',
                                    '201 1000000000.000008 execveat(3, "", [], NULL, AT_EMPTY_PATH) = 0'),
            'pid-reuse': replace(THREADED, '100 1000000000.000015 exit_group(0) = ?',
                                 '100 1000000000.000015 fork() = 200'),
            'worker-reexec': replace(THREADED, '200 1000000000.000011 exit_group(0) = ?',
                                     '200 1000000000.000011 execve("/build/archive-worker/bigbrotr-archive-worker", ["worker"], NULL) = 0'),
            'exec-away': replace(THREADED, '200 1000000000.000011 exit_group(0) = ?',
                                 '200 1000000000.000011 execve("/bin/true", ["true"], NULL) = 0'),
            'unknown-waitid-status': replace(PARENT_THREAD,
                'si_signo=SIGCHLD, si_code=CLD_EXITED, si_pid=300',
                'si_signo=SIGCHLD, si_code=CLD_EXITED_UNKNOWN, si_pid=300'),
        }
        for name, trace in mutations.items():
            with self.subTest(name=name), self.assertRaises(TraceError):
                audit_trace(trace)


if __name__ == '__main__':
    unittest.main()
