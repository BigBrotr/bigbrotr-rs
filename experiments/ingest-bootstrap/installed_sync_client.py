"""Independent private HTTP client and literal WebSocket relay inside the lab."""
import base64
import hashlib
import json
import math
import re
import socket
import struct
import threading
import time


REFUSAL_SHA256 = '64f3d2ad64cf6960aaa9908e8b85b21205ca89786cb2c21dacb3c4e8e4d1938f'
REFUSED_VISIBILITIES = {'sync-public-visibility': 1, 'sync-absent-visibility': 99,
    'sync-bootstrap-private-visibility': 2}


def refusal_frames(event, artifact):
    """Frozen signed kind22222 is outside kinds1, even with ephemeral retain."""
    assert hashlib.sha256(artifact).hexdigest() == REFUSAL_SHA256
    fixture = json.loads(artifact)
    assert fixture['format'] == 'bigbrotr-selector-signed-fixture/1'
    selected, = [entry for entry in fixture['events'] if entry['name'] == 'ephemeral']
    outside = selected['event']
    canonical = json.dumps([0, outside['pubkey'], outside['created_at'], outside['kind'],
        outside['tags'], outside['content']], ensure_ascii=False, separators=(',', ':')).encode()
    assert canonical.hex() == selected['canonical_hex']
    assert hashlib.sha256(canonical).hexdigest() == outside['id']
    assert outside['kind'] == 22222 and outside['id'] != event['id']
    assert len(bytes.fromhex(outside['sig'])) == 64
    return ({'id': event['id']}, outside, event)


def publish(directory, name, value):
    partial = directory / (name + '.partial')
    with partial.open('x') as stream:
        json.dump(value, stream, indent=2)
    partial.rename(directory / name)


COMMON_SELECTOR=dict(version=1,filters=[dict(kinds=[1],until=86399)],history_since=0,ephemeral='retain')
INITIAL_ENTRIES=(('request',0,86400,86400),('request',0,2,86400),('request',0,1,86400))


def frozen_horizon(selector,accepted_at_us):
    accepted=int(accepted_at_us)//1_000_000
    assert accepted>=0 and selector['filters']
    bounds=[]
    for part in selector['filters']:
        until=part.get('until',2**64-1)
        assert type(until) is int and 0<=until<=2**64-1
        bounds.append(min(accepted,until+1))
    return max(bounds)


def quantum_window(selector,accepted_at_us,*,expected_upper=None,expected_lower=None,window_end=None,width=86400):
    """Reconstruct a declared request, independently of SQL cursor metadata."""
    selector=selector or COMMON_SELECTOR
    origin=selector['history_since'];horizon=frozen_horizon(selector,accepted_at_us)
    assert type(origin) is int and 0<=origin<=2**64-1 and type(width) is int and width>0
    lower=origin if expected_lower is None else expected_lower
    assert type(lower) is int and origin<=lower<horizon
    end=min(lower+width,horizon) if window_end is None else window_end
    assert type(end) is int and lower<end<=horizon
    upper=end if expected_upper is None else expected_upper
    assert type(upper) is int and lower<upper<=end
    filters=[]
    for part in selector['filters']:
        since=max(lower,part.get('since',lower));until=min(upper-1,part.get('until',upper-1))
        if since<=until:filters.append(dict(part,since=since,until=until,limit=3))
    assert filters
    return lower,upper,horizon,filters


def quantum_status(observed,scope_id,accepted_at_us,*,known_plan=False,selector=None,
        expected_uppers=None,expected_entries=None,width=86400,budget=128):
    """Strict redacted profile2 observation against a predeclared entry recipe."""
    value=observed.get('linear_quantum')
    if value is None:
        assert not known_plan,'known durable plan missing from actor status'
        return None
    assert set(value)=={'format','quantum_number','scope_id','entry_kind','disposition','lower','upper',
        'window_end','horizon','window_width','query_budget','queries_spent','attempted_prefix','gap_count',
        'pending_work','pending_interval','result','terminal','gap','continuation_upper','retained_events',
        'classified_prefix','coverage'}
    assert value['format']=='bigbrotr-linear-quantum-status/2' and value['scope_id']==scope_id
    selector=selector or COMMON_SELECTOR;horizon=frozen_horizon(selector,accepted_at_us)
    number=int(value['quantum_number']);assert number>=1 and str(number)==value['quantum_number']
    assert expected_entries is None or expected_uppers is None,'conflicting declared quantum recipes'
    if expected_entries is None:
        uppers={1:min(selector['history_since']+width,horizon)} if expected_uppers is None else expected_uppers
        expected_entries={n:('request',selector['history_since'],upper,min(selector['history_since']+width,horizon))
            for n,upper in uppers.items()}
    assert all(type(n) is int and n>=1 for n in expected_entries)
    assert set(range(1,number+1))<=set(expected_entries),'declared entry prefix incomplete'
    entry=expected_entries[number];kind,lower,upper,end=entry[:4]
    assert all(type(v) is int for v in (lower,upper,end,width,budget)) and 1<=budget<=128
    assert (value['entry_kind'],value['lower'],value['upper'],value['window_end'],value['horizon'],value['window_width'])==(
        kind,str(lower),str(upper),str(end),str(horizon),str(width))
    assert value['coverage']=='pending'
    assert type(value['query_budget']) is int and value['query_budget']==budget
    spent=sum(expected_entries[n][0]=='request' for n in range(1,number+1))
    assert type(value['queries_spent']) is int and value['queries_spent']==spent<=budget
    assert type(value['gap_count']) is int and 0<=value['gap_count']<=spent
    attempted=value['attempted_prefix'];assert type(attempted) is str and str(int(attempted))==attempted
    assert min(selector['history_since'],horizon)<=int(attempted)<=horizon
    assert value['pending_work'] in ('request_pending','query_ready','budget_deferred','settled')
    interval=value['pending_interval']
    if value['pending_work']=='settled':assert interval is None and int(attempted)==horizon
    else:
        assert type(interval) is list and len(interval)==2 and all(type(n) is str and str(int(n))==n for n in interval)
        assert selector['history_since']<=int(interval[0])<int(interval[1])<=horizon
        assert (value['pending_work']=='budget_deferred')==(spent==budget and value['result']!='pending')
    if kind=='selector_exclusion':
        assert len(entry)==5 and entry[4] in ('selector_excluded','empty_target')
        assert value['disposition']==value['result']==entry[4]
        assert all(value[key] is None for key in ('terminal','gap','continuation_upper','retained_events','classified_prefix'))
    else:
        assert kind=='request' and len(entry)==4
        quantum_window(selector,accepted_at_us,expected_lower=lower,expected_upper=upper,window_end=end,width=width)
        if value['result']=='pending':
            assert value['pending_work']=='request_pending'
            assert all(value[key] is None for key in ('disposition','terminal','gap','continuation_upper','retained_events','classified_prefix'))
        else:
            assert value['result'] in ('linear_observed','relay_reported_exhaustion','interrupted')
            assert value['terminal'] in ('eose','closed','deadline','cancelled','frame_cap','event_cap','transport','decode','disconnected')
            assert value['disposition'] in ('retry','overlap','saturated_second','window_closed')
            continuation=value['continuation_upper'];assert type(continuation) is str and str(int(continuation))==continuation
            assert lower<=int(continuation)<=end
            if value['disposition']=='saturated_second':assert value['gap']=='saturated_second'
            else:assert (value['result']=='interrupted')==(value['gap'] is not None)
            assert type(value['classified_prefix']) is int and type(value['retained_events']) is int
            assert 0<=value['classified_prefix']<=value['retained_events']<=3
    closes=kind=='selector_exclusion' or value['disposition']=='window_closed' or (
        value['disposition']=='saturated_second' and int(value['continuation_upper'])==lower)
    if closes:
        expected_pending=None if end==horizon else [str(end),str(min(end+width,horizon))]
        expected_attempted=end
    else:
        next_upper=(int(value['continuation_upper']) if value['disposition'] in ('overlap','saturated_second') else upper)
        expected_pending=[str(lower),str(next_upper)];expected_attempted=lower
    expected_work=('request_pending' if value['result']=='pending' else 'settled' if expected_pending is None
        else 'budget_deferred' if spent==budget else 'query_ready')
    assert value['pending_interval']==expected_pending and value['pending_work']==expected_work
    assert value['attempted_prefix']==str(expected_attempted)
    return value


def common_final_status(observed,scope_id,accepted_at_us):
    """Exact final observation after the host verifies all three request terminals."""
    value=quantum_status(observed,scope_id,accepted_at_us,known_plan=True,
        selector=COMMON_SELECTOR,expected_entries=dict(enumerate(INITIAL_ENTRIES,1)))
    assert value==dict(format='bigbrotr-linear-quantum-status/2',quantum_number='3',scope_id=scope_id,
        entry_kind='request',disposition='window_closed',lower='0',upper='1',window_end='86400',
        horizon='86400',window_width='86400',query_budget=128,queries_spent=3,attempted_prefix='86400',
        gap_count=1,pending_work='settled',pending_interval=None,result='linear_observed',terminal='eose',
        gap=None,continuation_upper='86400',retained_events=0,classified_prefix=0,coverage='pending'), \
        'common final quantum summary differs from the declared three-request recipe'
    return value


def exact(connection, count):
    value = bytearray()
    while len(value) < count:
        part = connection.recv(count-len(value))
        if not part:
            raise EOFError('peer closed')
        value.extend(part)
    return bytes(value)


def read_frame(connection):
    first, second = exact(connection, 2)
    assert first in (0x81, 0x88), ('unexpected client frame', first)
    assert second & 128, 'client frame must be masked'
    size = second & 127
    assert size != 127, '64-bit command length exceeds fixture profile'
    if size == 126:
        size = int.from_bytes(exact(connection, 2), 'big')
        assert size >= 126
    assert size < 4096, 'client command bound'
    mask = exact(connection, 4)
    value = bytes(b ^ mask[i % 4] for i, b in enumerate(exact(connection, size)))
    return first & 15, value


def send_frame(connection, value):
    body = json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()
    assert len(body) < 65536
    header = bytes([0x81, len(body)]) if len(body) < 126 else b'\x81\x7e'+struct.pack('!H', len(body))
    connection.sendall(header+body)


def relay(directory, listener, finished, errors, signed, refusals=False, window=False,
          event_sender=None, expected_uppers=None):
    assert event_sender is None or (signed and not refusals and not window)
    split = None
    retained = (directory / 'retained-request-profile.json').exists()
    if retained:
        assert signed and not refusals and not window and event_sender is None
        assert not (directory / 'progress-context.json').exists()
        from installed_retained_transport import RetainedEvents
        split = RetainedEvents(directory)
    if (directory / 'progress-context.json').exists():
        assert signed and not refusals and not window and event_sender is None
        from installed_progress_transport import SplitEvents, ack_events
        split = SplitEvents(directory, json.loads((directory / 'event.jsonl').read_bytes()), 'initial',
            event_sequence=ack_events(directory))
    request_ids = set()
    uppers={1:86400} if expected_uppers is None else expected_uppers
    completed = None
    try:
        number = 0
        window_rounds = 0
        while not finished.is_set():
            if retained and number == 128:
                assert split.proof_sent, 'retained connection budget ended before proof'
                completed = dict(format='bigbrotr-retained-relay-completion/1',
                    reason='connection_limit', connections=number, proof_sent=True,
                    last_closed_monotonic_ns=trace['closed_monotonic_ns'],
                    last_closed_sha256=hashlib.sha256(
                        (directory/f'relay-closed-{number:03}.json').read_bytes()).hexdigest())
                break
            try:
                connection, remote = listener.accept()
            except socket.timeout:
                continue
            number += 1
            assert number <= 128 and remote[0] == '127.0.0.1'
            if signed and not refusals and not window and split is None and event_sender is None:
                assert number<=3,'settled common fixture opened a fourth socket'
            trace = dict(number=number, connected_at_us=time.time_ns()//1000,
                clock_resolution_us=math.ceil(time.clock_getres(time.CLOCK_REALTIME)*1_000_000))
            with connection:
                connection.settimeout(3)
                header = bytearray()
                while not header.endswith(b'\r\n\r\n'):
                    assert len(header) < 4096
                    header.extend(exact(connection, 1))
                lines = header.decode('ascii').split('\r\n')
                assert lines[0] == 'GET /installed-fixture HTTP/1.1'
                fields = {}
                for line in lines[1:]:
                    if line:
                        key, value = line.split(':', 1)
                        assert key.lower() not in fields
                        fields[key.lower()] = value.strip()
                assert fields['host'] == '127.0.0.1:7447'
                assert fields['upgrade'].lower() == 'websocket'
                assert fields['connection'].lower() == 'upgrade' and fields['sec-websocket-version'] == '13'
                key = fields['sec-websocket-key']
                assert len(base64.b64decode(key, validate=True)) == 16
                accept = base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
                connection.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+b'\r\n\r\n')
                opcode, raw = read_frame(connection)
                command = json.loads(raw)
                assert opcode == 1 and len(command) == 3 and command[0] == 'REQ'
                ident = command[1]
                assert re.fullmatch('[0-9a-f]{48}', ident) and int(ident[:32], 16) != 0
                assert ident[:32] not in request_ids, 'connection nonce reused'
                request_ids.add(ident[:32])
                assert command[2] == {'kinds': [1], 'since': 0, 'until': uppers[number]-1, 'limit': 3}
                uppers[number+1]=uppers[number]  # Known quiet interruptions retry this interval.
                trace.update(request=command, request_at_us=time.time_ns()//1000)
                if split is not None:
                    trace['request_monotonic_ns'] = time.monotonic_ns()
                ordinary_empty=signed and not refusals and not window and split is None and event_sender is None and number==3
                window_ready = window and (directory/'window-release').exists() and window_rounds < 2
                if retained:
                    selected = split.select(ident, number)
                    trace['retained_quiet'] = selected is None
                publish(directory, f'relay-open-{number:03}.json', trace)
                if not retained:
                    selected = split.select(ident, number) if split is not None else None
                if ordinary_empty:
                    assert command[2]==dict(kinds=[1],since=0,until=0,limit=3)
                    ident=command[1];trace['frames']=['EOSE'];trace['sent_at_us']=time.time_ns()//1000
                    send_frame(connection,['EOSE',ident])
                    opcode,raw=read_frame(connection);assert opcode==1 and json.loads(raw)==['CLOSE',ident]
                    opcode,raw=read_frame(connection);assert opcode==8 and raw==b''
                    connection.sendall(b'\x88\x00');connection.shutdown(socket.SHUT_WR);assert connection.recv(1)==b''
                    trace['completion']='request-and-websocket-close'
                elif (selected is not None if split is not None else signed and (number <= 2 or window_ready)):
                    event = json.loads((directory/'event.jsonl').read_bytes())
                    if split is not None:
                        values = split.describe(trace, selected)
                    elif event_sender is not None:
                        trace['sent_at_us'] = time.time_ns()//1000
                        trace['frames'] = event_sender(connection, ident, number)
                        assert trace['frames']==['signed','duplicate','EOSE']
                        uppers[number+1]=event['created_at']+1
                        values = ()
                    elif window_ready and number > 2:
                        from installed_sync_window import window_frames
                        window_rounds += 1
                        values, labels = window_frames(event, (directory/'selector-events.json').read_bytes(),
                            distinct=directory.name == 'sync-window-event' and window_rounds == 1)
                        trace.update(frames=labels+['EOSE'], window_sequence=window_rounds,
                            window_event_ids=[value['id'] for value in values])
                    elif refusals:
                        values = refusal_frames(event, (directory/'selector-events.json').read_bytes())
                        trace['frames'] = ['malformed-event', 'signed-outside-selector', 'signed', 'EOSE']
                        trace['outside_event_id'] = values[1]['id']
                        trace['selector_fixture_sha256'] = REFUSAL_SHA256
                    else:
                        values = (event, dict(event, sig='00'*64), event)
                        trace['frames'] = ['signed', 'forged-signature', 'duplicate', 'EOSE']
                    setup_closed=window and number<=2
                    if setup_closed:
                        trace['frames'][-1]='CLOSED'
                    if not setup_closed and not refusals and event_sender is None:
                        if split is not None:
                            # These frozen recipes declare whether every body can
                            # be classified; malformed holes retain their interval.
                            continuous=all(set(value)==set(event) for value in values)
                            times=[value['created_at'] for value in values if 'created_at' in value]
                            continuous=continuous and all(b<=a for a,b in zip(times,times[1:]))
                            if retained and trace['retained_phase']=='saturation':
                                # The pinned three-body page has an ascending
                                # pair; the durable oracle also requires its
                                # request65 refusal and unclassified suffix.
                                assert len(values)==3 and times[0]<times[1]
                        else:
                            continuous=not window or directory.name=='sync-window-source' or window_rounds==2
                            times=[event['created_at']]
                        if continuous and times:uppers[number+1]=min(times)+1
                    if window and directory.name=='sync-window-source' and window_rounds==2:
                        # The repeated boundary retains its second as a gap.
                        # The older remainder stays quiet for the owned signal.
                        uppers[number+1]=event['created_at']
                    if signed and not refusals and not window and split is None and event_sender is None and number==2:
                        uppers[3]=1  # Declare the next interval before emitting this page's EOSE.
                    if event_sender is None and split is None:
                        trace['sent_at_us'] = time.time_ns()//1000
                    for value in values:
                        send_frame(connection, ['EVENT', ident, value])
                    send_frame(connection, ['CLOSED', ident, 'fixture setup interruption'] if setup_closed else ['EOSE', ident])
                    opcode, raw = read_frame(connection)
                    if not setup_closed:
                        assert opcode == 1 and json.loads(raw) == ['CLOSE', ident]
                        opcode, raw = read_frame(connection)
                    assert opcode == 8 and raw == b''
                    connection.sendall(b'\x88\x00')
                    connection.shutdown(socket.SHUT_WR)
                    assert connection.recv(1) == b''
                    trace['completion'] = 'request-and-websocket-close'
                else:
                    trace['frames'] = []
                    # Deliberately no EVENT or EOSE. A bounded cancelled receive
                    # must destroy this connection; it cannot reuse the session.
                    assert connection.recv(1) == b'', 'quiet quantum did not destroy socket'
                    trace['completion'] = 'quiet-eof'
                trace['closed_at_us'] = time.time_ns()//1000
                if split is not None:
                    trace['closed_monotonic_ns'] = time.monotonic_ns()
                assert trace['closed_at_us']-trace['request_at_us'] < 3_000_000
                publish(directory, f'relay-closed-{number:03}.json', trace)
    except EOFError as error:
        if (directory/'stop-requested').exists():
            publish(directory, 'relay-shutdown.json', dict(error=str(error), phase='incomplete handshake during owned signal'))
        else:
            errors.append(type(error).__name__+': '+str(error))
            publish(directory, 'relay-error.json', dict(error=errors[-1]))
    except BaseException as error:
        errors.append(type(error).__name__+': '+str(error))
        publish(directory, 'relay-error.json', dict(error=errors[-1]))
    finally:
        listener.close()
        if completed is not None:
            publish(directory, 'relay-finite-complete.json', dict(completed,
                listener_closed=True, completed_monotonic_ns=time.monotonic_ns()))


def no_connections(directory, listener, finished, errors):
    """Keep a real listening socket throughout the refused job and owned shutdown."""
    started = time.time_ns()//1000
    connections = 0
    publish(directory, 'relay-monitor-started.json', dict(started_at_us=started))
    try:
        while not finished.is_set():
            try:
                connection, remote = listener.accept()
            except socket.timeout:
                continue
            connections += 1
            connection.close()
            raise AssertionError('source with refused visibility dialed '+remote[0])
    except BaseException as error:
        errors.append(type(error).__name__+': '+str(error))
        publish(directory, 'relay-error.json', dict(error=errors[-1]))
    finally:
        listener.close()
        publish(directory, 'relay-monitor-ended.json', dict(started_at_us=started,
            finished_at_us=time.time_ns()//1000, connections=connections))


def relay_listener(address=('127.0.0.1', 7447)):
    listener = socket.socket()
    try:
        # Completed rounds can leave accepted sockets in TIME_WAIT across cases
        # sharing this namespace. REUSEADDR permits restart, not a second live
        # listener; deliberately do not enable REUSEPORT or retry another port.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(address)
        listener.listen(2)
        listener.settimeout(.1)
        return listener
    except BaseException:
        listener.close()
        raise


def run(directory, request, login, logout):
    refusals = directory.name == 'sync-peer-refusals'
    expiry = directory.name == 'sync-expired-before-valid'
    window = directory.name in ('sync-window-event', 'sync-window-source')
    signed = directory.name == 'sync-signed' or refusals or expiry or window
    visibility_refusal = directory.name in REFUSED_VISIBILITIES
    assert signed or visibility_refusal or directory.name == 'sync-quiet'
    observations = []
    logged_in, session, saved, auth, _ = login(('policy.change', 'roles.control', 'status.read'))

    def call(path, value, key):
        status, body = request('POST', '/management/v1/'+path, value, {**auth, 'Idempotency-Key': key})
        answer = json.loads(body)
        observations.append(dict(path=path, input=value, key=key, status=status, body=answer))
        assert status == 202 and answer['created'] and answer['command_key'] == key, (status, answer)
        return answer

    def barrier(name, deadline=None):
        publish(directory, name+'.json', observations)
        deadline = time.monotonic()+22 if deadline is None else deadline
        while not (directory/(name+'.continue')).exists():
            assert not errors, errors
            assert time.monotonic() < deadline, name
            time.sleep(.01)

    visibility = json.loads((directory/'visibility.json').read_text())
    assert visibility['ordinal'] == str(REFUSED_VISIBILITIES.get(directory.name, 3))
    revisions = dict(expected_config_revision='1', expected_policy_revision='1')
    endpoint = call('relay-endpoints', dict(url='WS://127.0.0.1:7447/a/../installed-fixture', **revisions), '91'*16)
    enrollment = call('relay-enrollments', dict(endpoint_id=endpoint['endpoint_id'], visibility=visibility,
        route={'class': 'private', 'cidr': '127.0.0.1/32'}, enabled=True, expected_generation=None, **revisions), '92'*16)
    assert enrollment['generation'] == '1' and enrollment['enabled'] is True
    scope = call('acquisition-scopes', dict(endpoint_id=endpoint['endpoint_id'], access_scope_id=enrollment['access_scope_id'],
        visibility=visibility, selector=COMMON_SELECTOR, **revisions), 'a1'*16)
    listener = relay_listener()
    finished, errors = threading.Event(), []
    expected_uppers={1:86400}
    worker = threading.Thread(target=no_connections if visibility_refusal else relay,
        args=(directory, listener, finished, errors) if visibility_refusal else (directory, listener, finished, errors, signed, refusals, window),
        kwargs={} if visibility_refusal else dict(expected_uppers=expected_uppers))
    worker.start()
    deferred_worker = None
    try:
        barrier('prepared')
        if expiry:
            from installed_sync_expiry import DURATION_US, KEY, suspended_cli
            call('jobs', dict(kind='acquisition', scope_id=scope['scope_id'],
                deadline_us=str(DURATION_US), **revisions), KEY)
            with suspended_cli(directory, publish) as deadline:
                barrier('expiry-accepted', deadline)
            barrier('expiry-continued')
        receipt = call('jobs', dict(kind='acquisition', scope_id=scope['scope_id'], deadline_us='120000000', **revisions), 'b1'*16)
        assert receipt['execution'] == 'unobserved' and receipt['authority'] == 'acceptance-only'
        barrier('submitted')
        status, raw = request('GET', '/management/v1/jobs/'+receipt['job_id'], headers=auth)
        observed = json.loads(raw)
        quantum_status(observed,scope['scope_id'],receipt['accepted_at_us'],known_plan=True,expected_uppers=expected_uppers)
        if signed and not refusals and not window and not (directory/'progress-context.json').exists():
            common_final_status(observed,scope['scope_id'],receipt['accepted_at_us'])
        assert status == 200 and {k:v for k,v in observed.items() if k!='linear_quantum'} == dict(format='bigbrotr-job-status/1', job_id=receipt['job_id'],
            kind='acquisition', state='queued', job_revision='2', checkpoint_revision='0',
            created_at_us=receipt['accepted_at_us'], completed_at_us=None, observed_at_us=observed['observed_at_us'],
            reason_code=None, authority='observation-only')
        assert int(observed['observed_at_us']) >= int(receipt['accepted_at_us'])
        if window:
            deferred_receipt = None
            if directory.name == 'sync-window-source':
                from installed_sync_window import SECOND_ENDPOINT
                monitored = directory/'deferred-source'; monitored.mkdir()
                deferred_listener = relay_listener(('127.0.0.1', 7448))
                deferred_worker = threading.Thread(target=no_connections,
                    args=(monitored, deferred_listener, finished, errors))
                deferred_worker.start()
                other = call('relay-endpoints', dict(url=SECOND_ENDPOINT, **revisions), '93'*16)
                other_enrollment = call('relay-enrollments', dict(endpoint_id=other['endpoint_id'], visibility=visibility,
                    route={'class': 'private', 'cidr': '127.0.0.1/32'}, enabled=True,
                    expected_generation=None, **revisions), '94'*16)
                other_scope = call('acquisition-scopes', dict(endpoint_id=other['endpoint_id'],
                    access_scope_id=other_enrollment['access_scope_id'], visibility=visibility,
                    selector=COMMON_SELECTOR,
                    **revisions), 'a2'*16)
                deferred_receipt = call('jobs', dict(kind='acquisition', scope_id=other_scope['scope_id'],
                    deadline_us='120000000', **revisions), 'b2'*16)
            barrier('window-commands')
            if deferred_receipt:
                status, raw = request('GET', '/management/v1/jobs/'+deferred_receipt['job_id'], headers=auth)
                deferred_status = json.loads(raw)
                quantum_status(deferred_status,other_scope['scope_id'],deferred_receipt['accepted_at_us'])
                assert status == 200 and {k:v for k,v in deferred_status.items() if k!='linear_quantum'} == dict({k:v for k,v in observed.items() if k!='linear_quantum'},
                    job_id=deferred_receipt['job_id'], created_at_us=deferred_receipt['accepted_at_us'],
                    observed_at_us=deferred_status['observed_at_us'])
                assert int(deferred_status['observed_at_us']) >= int(deferred_receipt['accepted_at_us'])
                publish(directory, 'window-job-status.json', deferred_status)
            previous=observed
            status,raw=request('GET','/management/v1/jobs/'+receipt['job_id'],headers=auth)
            observed=json.loads(raw)
            quantum_status(observed,scope['scope_id'],receipt['accepted_at_us'],known_plan=True,expected_uppers=expected_uppers)
            assert status==200 and {k:v for k,v in observed.items() if k!='linear_quantum'}==dict(
                {k:v for k,v in previous.items() if k!='linear_quantum'},observed_at_us=observed['observed_at_us'])
            assert int(observed['observed_at_us'])>=int(previous['observed_at_us'])
        logged_out = logout(session, saved, auth)
        barrier('commands-finished')  # Host stops Serve while our relay remains available.
    finally:
        finished.set()
        worker.join(4)
        assert not worker.is_alive(), 'relay worker did not join'
        if deferred_worker is not None:
            deferred_worker.join(1)
            assert not deferred_worker.is_alive(), 'deferred-source monitor did not join'
    assert not errors, errors
    return dict(login=logged_in, logout=logged_out, observations=observations, job_status=observed)
