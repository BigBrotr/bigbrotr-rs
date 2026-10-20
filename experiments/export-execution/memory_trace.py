"""Offline, fail-closed worker lifetime audit for one complete finite phase.

Required acquisition: the independently pinned strace 6.1 instrument, with
``-f -ttt -e trace=process -s 256 -o FILE``, without injection. Root owns that
invocation, the original file and all external custody. This parser proves only
what its text records: process creation lineage, exact-path successful exec,
terminal exit and a matching successful parent wait. A trace event's timestamp
and line identify an observation; neither is a /proc start_ticks identity.

Only decoded fork/vfork/clone/clone3, execve, wait4/waitid and exit calls are
supported. PID reuse, nonleader exec, alternative parentage/namespaces, detached
children, truncated records and unknown forms refuse. Threads inherit their
creator's process identity; their exits do not establish process reaping.
Every process child must have a successful exec and a parent reap; a child that
only runs an inherited image is unsupported. minimum_workers=0 permits phases
without workers, but never relaxes the checks on any observed worker.
The initial tracee's creation and reaping by strace are outside this proof.
No container, cgroup, backend, mount, physical-state or memory claim is made.
"""
import hashlib
import re

WORKER = '/build/archive-worker/bigbrotr-archive-worker'
TRACER_SHA256 = '509ea4b8a7eb303990155f013e1e288df1054d63e5c81b11bc3c31b5905b7458'
TRACE_ARGUMENTS = ('-f', '-ttt', '-e', 'trace=process', '-s', '256')
_FLAGS = {
    'CLONE_VM', 'CLONE_FS', 'CLONE_FILES', 'CLONE_SIGHAND', 'CLONE_THREAD',
    'CLONE_SYSVSEM', 'CLONE_SETTLS', 'CLONE_PARENT_SETTID',
    'CLONE_CHILD_CLEARTID', 'CLONE_CHILD_SETTID', 'CLONE_VFORK',
}


class TraceError(ValueError):
    """The supplied trace cannot establish the narrow worker lifetime claim."""


def require(condition, message):
    if not condition:
        raise TraceError(message)


def _arguments(value):
    """Split decoded syscall arguments without treating quoted commas as fields."""
    parts, stack, quoted, escaped, start = [], [], False, False, 0
    for offset, char in enumerate(value):
        if quoted:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in '([{':
            stack.append(char)
        elif char in ')]}':
            require(stack and stack.pop() == {')': '(', ']': '[', '}': '{'}[char],
                    'unbalanced syscall arguments')
        elif char == ',' and not stack:
            parts.append(value[start:offset].strip())
            start = offset + 1
    require(not stack and not quoted, 'truncated syscall arguments')
    return parts + [value[start:].strip()] if value else []


def _events(text):
    require(isinstance(text, str) and text and text.endswith('\n') and '\r' not in text
            and '\0' not in text, 'complete newline-terminated trace text required')
    pending, events, last = {}, [], {}
    for number, line in enumerate(text.splitlines(), 1):
        match = re.fullmatch(r'([1-9][0-9]*)\s+([0-9]+\.[0-9]{6}) (.+)', line)
        require(match is not None, f'unsupported trace line {number}')
        pid, at, body = int(match[1]), match[2], match[3]
        seconds, micros = at.split('.')
        clock = int(seconds) * 1_000_000 + int(micros)
        require(clock >= last.get(pid, 0), f'backwards task timestamp at line {number}')
        last[pid] = clock
        event = dict(pid=pid, line=number, at=at, clock=clock,
                     end_line=number, completed_at=at, end_clock=clock)
        if body.endswith(' <unfinished ...>'):
            call = re.match(r'([a-z0-9_]+)\(', body)
            require(call is not None and pid not in pending, 'nested or malformed unfinished call')
            pending[pid] = (call[1], body.removesuffix(' <unfinished ...>'), event)
            continue
        resumed = re.fullmatch(r'<\.\.\. ([a-z0-9_]+) resumed>(.*)', body)
        if resumed:
            require(pid in pending and pending[pid][0] == resumed[1], 'orphan/mismatched resumed call')
            _, prefix, original = pending.pop(pid)
            body = prefix + resumed[2]
            event = dict(original, end_line=number, completed_at=at, end_clock=clock)
        elif body.startswith('--- '):
            require(re.fullmatch(r'--- SIG[A-Z0-9]+ \{.*\} ---', body), 'unsupported signal record')
            event.update(kind='signal')
            events.append(event)
            continue
        else:
            require(pid not in pending, 'unfinished call lacks matching resume')
        terminal = re.fullmatch(r'\+\+\+ exited with ([0-9]+) \+\+\+', body)
        if terminal:
            code = int(terminal[1])
            require(code <= 255, 'invalid terminal exit code')
            event.update(kind='terminal', code=code)
        else:
            require('...' not in body, 'truncated or unsupported trace record')
            call = re.fullmatch(r'([a-z0-9_]+)\((.*)\)\s+= (.+)', body)
            require(call is not None, f'unsupported call/terminal at line {number}')
            result = re.fullmatch(r'(-?[0-9]+|\?)(?: ([A-Z][A-Z0-9_]+)(?: \(.*\))?)?', call[3])
            require(result is not None, 'unsupported syscall result')
            value = None if result[1] == '?' else int(result[1])
            error = result[2]
            require((value == -1 and error is not None) or
                    (value is None) or (value >= 0 and error is None), 'ambiguous syscall result')
            event.update(kind='call', name=call[1], args=_arguments(call[2]), result=value, error=error)
        events.append(event)
    require(not pending, 'trace ends with unresolved unfinished calls')
    return sorted(events, key=lambda event: event['line'])


def _thread(call):
    name, args = call['name'], call['args']
    if name in ('fork', 'vfork'):
        require(not args, 'unsupported fork arguments')
        return False
    require(name in ('clone', 'clone3'), 'unknown creation syscall')
    raw = ', '.join(args)
    flags = re.findall(r'(?:^|[,{] *)flags=([^,}]+)', raw)
    require(len(flags) == 1, 'missing decoded clone flags')
    tokens = flags[0].split('|')
    require(len(tokens) == len(set(tokens)), 'duplicate clone flag')
    flags = set(tokens)
    if name == 'clone3':
        signals = re.findall(r'(?:^|[,{] *)exit_signal=([^,}]+)', raw)
        require(len(signals) == 1 and signals[0] in ('0', 'SIGCHLD'), 'unsupported clone3 exit signal')
        signal = signals[0]
        if flags == {'0'}:
            flags = set()
    else:
        signal = 'SIGCHLD' if 'SIGCHLD' in flags else '0'
        flags.discard('SIGCHLD')
    require(flags <= _FLAGS, 'unsupported clone flags or parentage')
    thread = 'CLONE_THREAD' in flags
    require((thread and {'CLONE_VM', 'CLONE_SIGHAND'} <= flags and signal == '0'
             and 'CLONE_VFORK' not in flags) or (not thread and signal == 'SIGCHLD'),
            'unsupported process/thread creation')
    return thread


def _wait(call):
    """Return a reaped PID and exit code; polling/errors never count as reaping."""
    args, result, name = call['args'], call['result'], call['name']
    require(name in ('wait4', 'waitid'), 'unsupported wait')
    if result == -1 or (result is None and call['error'] in ('ERESTARTSYS', 'ERESTARTNOINTR')):
        return None
    if name == 'wait4':
        require(len(args) == 4 and re.fullmatch(r'-?[0-9]+', args[0]), 'unsupported wait4 arguments')
        wanted = int(args[0])
        require(wanted == -1 or wanted > 0, 'unsupported wait4 selector')
        require(args[2] in ('0', 'WNOHANG'), 'unsupported wait4 options')
        if result == 0:
            require(args[2] == 'WNOHANG', 'zero wait without WNOHANG')
            return None
        require(result is not None and result > 0 and (wanted == -1 or wanted == result),
                'wait4 result/selector mismatch')
        status = re.fullmatch(r'\[\{WIFEXITED\(s\) && WEXITSTATUS\(s\) == ([0-9]+)\}\]', args[1])
        require(status is not None, 'wait4 lacks decoded terminal exit status')
        return result, int(status[1])
    require(result == 0 and len(args) == 5 and args[0] in ('P_PID', 'P_ALL')
            and re.fullmatch(r'[0-9]+', args[1]), 'unsupported waitid arguments')
    options = args[3].split('|')
    require(len(options) == len(set(options)) and set(options) <= {'WEXITED', 'WNOHANG'}
            and 'WEXITED' in options, 'waitid must actually reap (no WNOWAIT)')
    ids = re.findall(r'(?:^|[,{] *)si_pid=([0-9]+)', args[2])
    if args[2] == '{}' or ids == ['0']:
        require('WNOHANG' in options, 'empty waitid without WNOHANG')
        return None
    codes = re.findall(r'(?:^|[,{] *)si_status=([0-9]+)', args[2])
    require(len(ids) == len(codes) == 1
            and re.findall(r'(?:^|[,{] *)si_code=([^,}]+)', args[2]) == ['CLD_EXITED']
            and re.findall(r'(?:^|[,{] *)si_signo=([^,}]+)', args[2]) == ['SIGCHLD'],
            'waitid lacks decoded terminal exit')
    child = int(ids[0])
    require(child > 0 and ((args[0] == 'P_PID' and int(args[1]) == child)
                          or (args[0] == 'P_ALL' and args[1] == '0')), 'waitid selector mismatch')
    return child, int(codes[0])


def _ref(event):
    return {key: event[key] for key in ('pid', 'line', 'at', 'end_line', 'completed_at')}


def audit_trace(text, *, minimum_workers=1):
    """Return JSON evidence or raise TraceError; never executes or opens anything."""
    require(type(minimum_workers) is int and minimum_workers in (0, 1), 'minimum_workers must be 0 or 1')
    events = _events(text)
    tasks, births, execs, terminals, waits, exits = {}, {}, {}, {}, [], {}
    allowed = {'fork', 'vfork', 'clone', 'clone3', 'execve', 'wait4', 'waitid', 'exit', 'exit_group'}
    for event in events:
        pid = event['pid']
        tasks.setdefault(pid, []).append(event)
        if event['kind'] == 'terminal':
            require(pid not in terminals, 'duplicate terminal/PID reuse')
            terminals[pid] = event
            continue
        if event['kind'] != 'call':
            continue
        name, args, result = event['name'], event['args'], event['result']
        require(name in allowed, 'unsupported process syscall: ' + name)
        if name in ('fork', 'vfork', 'clone', 'clone3'):
            if result == -1:
                continue
            require(result is not None and result > 0 and result != pid, 'unknown process creation result')
            thread = _thread(event)
            require(result not in births, 'reused task PID')
            births[result] = dict(event=event, thread=thread, creator=pid)
        elif name == 'execve':
            require(len(args) == 3 and re.fullmatch(r'"[^"\\]*"', args[0]), 'ambiguous executable path')
            path = args[0][1:-1]
            require(path.startswith('/'), 'nonabsolute executable path')
            require(path == WORKER or path.rsplit('/', 1)[-1] != WORKER.rsplit('/', 1)[-1],
                    'worker executable under another path')
            require(result in (0, -1), 'unknown exec result')
            if path == WORKER:
                require(result == 0, 'worker exec failed')
            if result == 0:
                execs.setdefault(pid, []).append(dict(event, path=path))
        elif name in ('wait4', 'waitid'):
            reaped = _wait(event)
            if reaped is not None:
                waits.append((event, *reaped))
        else:
            require(len(args) == 1 and re.fullmatch(r'[0-9]+', args[0])
                    and int(args[0]) <= 255 and result is None and event['error'] is None,
                    'unsupported exit call')
            require(pid not in exits, 'duplicate exit request')
            exits[pid] = int(args[0])

    roots = set(tasks) - set(births)
    require(len(roots) == 1 and set(births) <= set(tasks), 'missing birth/unknown task/root')
    root, = roots
    require(execs.get(root) and execs[root][0]['line'] == tasks[root][0]['line']
            and execs[root][0]['path'] != WORKER, 'initial tracee must begin with nonworker exec')
    require(set(terminals) == set(tasks), 'missing terminal record')
    groups = {root: root}

    def group(pid, visiting):
        require(pid not in visiting, 'cyclic process lineage')
        if pid not in groups:
            birth = births[pid]
            creator_group = group(birth['creator'], visiting | {pid})
            groups[pid] = creator_group if birth['thread'] else pid
        return groups[pid]

    for pid, records in tasks.items():
        group(pid, set())
        require(pid in terminals and records[-1] is terminals[pid], 'missing terminal or event after exit')
        require(pid not in exits or exits[pid] == terminals[pid]['code'], 'exit request/terminal mismatch')
        if pid in births:
            birth = births[pid]['event']
            require(birth['line'] < records[0]['line'] and birth['clock'] <= records[0]['clock']
                    and birth['end_line'] < terminals[birth['pid']]['line'], 'birth/lifetime ordering')
        require(groups[pid] != pid or execs.get(pid), 'process lacks successful exec record')
        for execution in execs.get(pid, []):
            require(groups[pid] == pid, 'nonleader exec is unsupported')
            require(execution['end_line'] < terminals[pid]['line'], 'exec did not complete before terminal')

    reaped = {}
    for event, child, code in waits:
        require(child in births and groups[child] == child, 'wait names unknown child or thread')
        parent = groups[births[child]['creator']]
        require(groups[event['pid']] == parent and child not in reaped, 'wrong parent or duplicate reap')
        require(terminals[child]['end_line'] < event['end_line']
                and terminals[child]['clock'] <= event['end_clock']
                and terminals[child]['code'] == code, 'wait/terminal mismatch')
        reaped[child] = event
    require(set(reaped) == {pid for pid in births if groups[pid] == pid}, 'unreaped traced process')
    for child, event in reaped.items():
        require(all(terminals[tid]['line'] < event['end_line']
                    for tid in tasks if groups[tid] == child), 'process reaped before all its tasks exit')

    workers = []
    for pid, executions in execs.items():
        selected = [event for event in executions if event['path'] == WORKER]
        if not selected:
            continue
        require(len(selected) == 1 and executions[-1] is selected[0], 'unknown worker exec generation')
        execution = selected[0]
        require(pid in births and not births[pid]['thread'] and terminals[pid]['code'] == 0,
                'worker lacks process birth or successful exit')
        threads = sorted(tid for tid in tasks if groups[tid] == pid and tid != pid)
        require(all(terminals[tid]['code'] == 0 and terminals[tid]['line'] < reaped[pid]['end_line']
                    for tid in threads), 'worker thread not terminal before process reap')
        for child, birth in births.items():
            require(not (groups[birth['creator']] == pid and not birth['thread']),
                    'worker fork creates an unclassified inherited image')
        ancestry, ancestor = [], pid
        while ancestor != root:
            birth = births[ancestor]
            ancestry.append(dict(pid=ancestor, birth=_ref(birth['event']),
                                 creator_tid=birth['creator'], thread=birth['thread']))
            ancestor = birth['creator']
        workers.append(dict(pid=pid, parent_pid=groups[births[pid]['creator']],
                            birth=_ref(births[pid]['event']), exec=_ref(execution),
                            terminal=dict(_ref(terminals[pid]), exit_code=0),
                            reap=_ref(reaped[pid]), thread_tids=threads, lineage=list(reversed(ancestry))))
    require(len(workers) >= minimum_workers, 'no exact-path worker process observed')
    return dict(format='bigbrotr-export-worker-process-trace/1', status='PASS_WORKER_PROCESS_TRACE',
                trace_text_sha256=hashlib.sha256(text.encode('utf-8')).hexdigest(),
                worker_executable=WORKER, workers=sorted(workers, key=lambda row: row['exec']['line']),
                minimum_workers=minimum_workers, worker_count=len(workers),
                task_count=len(tasks), process_count=len(set(groups.values())),
                thread_count=sum(pid != owner for pid, owner in groups.items()),
                root=dict(pid=root, exec=_ref(execs[root][0]),
                          terminal=dict(_ref(terminals[root]), exit_code=terminals[root]['code'])),
                identity='trace birth/exec timestamps and lineage; not /proc start_ticks',
                scope='Complete traced worker exit-0/parent-reap evidence only; external custody not established')
