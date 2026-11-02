"""Six literal capped pages; private socket evidence, never cursor authority."""
import base64
import errno
import hashlib
import json
import math
import re
import socket
import time

from installed_sync_client import exact,read_frame,send_frame,publish,REFUSAL_SHA256

IMPORT_SHA256='c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae'
WINDOW_SHA256='b6ae647fbaa641c8bb2c4ae7dbcf12987a8b3bd01cd14eecbf15476c344111cc'
PROFILE=dict(format='bigbrotr-linear-capped-fixture/2',requests=6,scopes=2,requested_limit=3,relay_limit=2)
ADOPTION_PROFILE=dict(format='bigbrotr-linear-capped-adoption-fixture/1',requests=5,scopes=1,
    requested_limit=3,relay_limit=2)
SELECTORS=(dict(version=1,filters=[dict(kinds=[1],since=23,until=86399)],history_since=0,ephemeral='retain'),
    dict(version=1,filters=[dict(kinds=[1],until=86399)],history_since=0,ephemeral='retain'))
ENTRIES=((('request',0,86400,86400),('request',0,24,86400),
          ('selector_exclusion',0,23,86400,'selector_excluded')),
         (('request',0,86400,86400),('request',0,24,86400),
          ('request',0,2,86400),('request',0,1,86400)))
UPPERS=tuple(tuple(entry[2] for entry in entries if entry[0]=='request') for entries in ENTRIES)
TURN_SCOPES=(0,0,1,1,1,1)
PAGES=((0,1),(1,),(0,1),(3,2),(2,),())


def corpus(directory):
    artifact=(directory/'selector-events.json').read_bytes()
    original=(directory/'event.jsonl').read_bytes()
    assert hashlib.sha256(artifact).hexdigest()==REFUSAL_SHA256
    assert hashlib.sha256(original).hexdigest()==IMPORT_SHA256
    value=json.loads(artifact);events=[]
    assert value['format']=='bigbrotr-selector-signed-fixture/1'
    for name in ('outside-request-window','included'):
        entry,=[entry for entry in value['events'] if entry['name']==name]
        event=entry['event']
        canonical=json.dumps([0,event['pubkey'],event['created_at'],event['kind'],event['tags'],event['content']],
            ensure_ascii=False,separators=(',',':')).encode()
        assert canonical.hex()==entry['canonical_hex']
        assert hashlib.sha256(canonical).hexdigest()==event['id']
        events.append(event)
    events.append(json.loads(original))
    window=(directory/'linear-window-events.json').read_bytes()
    assert hashlib.sha256(window).hexdigest()==WINDOW_SHA256
    extra=json.loads(window)
    assert extra['format']=='bigbrotr-linear-window-events/1'
    alternate,=[row for row in extra['events'] if row['name']=='same-second-alternate']
    event=alternate['event']
    encoded=json.dumps([0,event['pubkey'],event['created_at'],event['kind'],event['tags'],event['content']],
        ensure_ascii=False,separators=(',',':')).encode()
    assert encoded.hex()==alternate['canonical_hex'] and hashlib.sha256(encoded).hexdigest()==event['id']
    events.append(event)
    assert [event['created_at'] for event in events]==[24,23,1,23]
    assert len({event['id'] for event in events})==4
    assert all(event['kind']==1 and len(bytes.fromhex(event['sig']))==64 for event in events)
    return events


def exchange_page(directory,connection,number,predicate,values,nonces,**labels):
    """One fixed finite page through the real shared WebSocket wire contract."""
    assert 0<=len(values)<=2 and predicate['limit']==3
    connection.settimeout(3)
    connected=time.time_ns()//1000;header=bytearray()
    while not header.endswith(b'\r\n\r\n'):
        assert len(header)<4096;header.extend(exact(connection,1))
    lines=header.decode('ascii').split('\r\n');fields={}
    assert lines[0]=='GET /installed-fixture HTTP/1.1'
    for line in lines[1:]:
        if line:
            key,value=line.split(':',1);assert key.lower() not in fields;fields[key.lower()]=value.strip()
    assert fields['host']=='127.0.0.1:7447' and fields['upgrade'].lower()=='websocket'
    assert fields['connection'].lower()=='upgrade' and fields['sec-websocket-version']=='13'
    key=fields['sec-websocket-key'];assert len(base64.b64decode(key,validate=True))==16
    accept=base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
    connection.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+b'\r\n\r\n')
    opcode,encoded=read_frame(connection);request=json.loads(encoded)
    assert opcode==1 and len(request)==3 and request[0]=='REQ' and request[2]==predicate
    ident=request[1]
    assert re.fullmatch('[0-9a-f]{48}',ident) and int(ident[:32],16) and ident[:32] not in nonces
    nonces.add(ident[:32])
    trace=dict(number=number,request=request,connected_at_us=connected,request_at_us=time.time_ns()//1000,
        clock_resolution_us=math.ceil(time.clock_getres(time.CLOCK_REALTIME)*1_000_000),**labels)
    publish(directory,f'relay-open-{number:03}.json',trace)
    trace.update(sent_at_us=time.time_ns()//1000,frames=['signed']*len(values)+['EOSE'],
        event_ids=[event['id'] for event in values],
        event_json_hex=[json.dumps(event,ensure_ascii=False,separators=(',',':')).encode().hex() for event in values])
    for event in values:send_frame(connection,['EVENT',ident,event])
    send_frame(connection,['EOSE',ident])
    opcode,encoded=read_frame(connection)
    assert opcode==1 and json.loads(encoded)==['CLOSE',ident]
    opcode,encoded=read_frame(connection);assert opcode==8 and encoded==b''
    connection.sendall(b'\x88\x00');connection.shutdown(socket.SHUT_WR)
    assert connection.recv(1)==b''
    trace.update(closed_at_us=time.time_ns()//1000,completion='request-and-websocket-close')
    assert trace['closed_at_us']-trace['request_at_us']<3_000_000
    publish(directory,f'relay-closed-{number:03}.json',trace)
    return trace


def accept_nowait(listener):
    """Reach kernel accept without a timeout poll; restore the owned mode."""
    timeout=listener.gettimeout()
    try:
        listener.setblocking(False)
        return listener.accept()
    finally:
        listener.settimeout(timeout)


def relay(directory,listener,finished,errors,*,adoption=False):
    """The adoption recipe keeps this same listener across the owner change.

    The source-only controls exercise transport, never authorize a continuation.
    Only the installed host's SQL/process/native observations can qualify that.
    """
    assert type(adoption) is bool
    profile=ADOPTION_PROFILE if adoption else PROFILE
    profile_name='linear-capped-adoption-profile.json' if adoption else 'linear-capped-profile.json'
    prefix='capped-adoption-peer' if adoption else 'capped-peer'
    count=profile['requests']
    pages=((0,1),(1,),(2,),(2,),()) if adoption else PAGES
    uppers=(86400,24,23,2,1) if adoption else tuple(UPPERS[scope][TURN_SCOPES[:n+1].count(scope)-1]
        for n,scope in enumerate(TURN_SCOPES))
    number=0;nonces=set();completed=False
    try:
        assert json.loads((directory/profile_name).read_text())==profile
        events=corpus(directory)
        while not finished.is_set():
            probe=number==count and not completed
            try:connection,remote=accept_nowait(listener) if probe else listener.accept()
            except BlockingIOError as error:
                if not probe or error.errno not in (errno.EAGAIN,errno.EWOULDBLOCK):raise
                if finished.is_set():continue
                # The same listener accepted every page through source EOF.
                # A direct empty-queue accept now proves it still listens;
                # SO_ACCEPTCONN getsockopt is unsupported on Darwin.
                listening=listener.fileno()>=0
                assert listening,'capped listener closed before completion'
                publish(directory,prefix+'-complete.json',dict(format=profile['format'],requests=count,
                    listener_closed=False,listening=listening,completed_at_us=time.time_ns()//1000))
                completed=True
                continue
            except socket.timeout:
                if probe:raise
                continue
            number+=1
            with connection:
                assert number<=count,'bounded continuation opened an extra socket'
                assert remote[0]=='127.0.0.1'
                scope=0 if adoption else TURN_SCOPES[number-1]
                turn=number if adoption else TURN_SCOPES[:number].count(scope)
                upper=uppers[number-1]
                values=[events[index] for index in pages[number-1]]
                assert bool(values)==(number<count),'only the declared older remainder is empty'
                exchange_page(directory,connection,number,
                    dict(kinds=[1],since=23 if not adoption and scope==0 else 0,until=upper-1,limit=3),
                    values,nonces,scope_number=scope,scope_turn=turn)
    except BaseException as error:
        errors.append(type(error).__name__+': '+str(error));publish(directory,'relay-error.json',dict(error=errors[-1]))
    finally:
        listener.close()
        publish(directory,prefix+'-joined.json',dict(format=profile['format'],requests=number,
            listener_closed=True,joined_at_us=time.time_ns()//1000,errors=len(errors)))
