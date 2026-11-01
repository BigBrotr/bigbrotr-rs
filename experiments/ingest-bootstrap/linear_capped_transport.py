"""Five literal capped pages; private socket evidence, never cursor authority."""
import base64
import hashlib
import json
import math
import re
import socket
import time

from installed_sync_client import exact,read_frame,send_frame,publish,REFUSAL_SHA256

IMPORT_SHA256='c30658a75b2b913e509b7618f36ebae75fa1d5680dc8fe4e322aae638d8affae'
PROFILE=dict(format='bigbrotr-linear-capped-fixture/1',requests=5,scopes=2,requested_limit=3,relay_limit=2)
SELECTORS=(dict(version=1,filters=[dict(kinds=[1],since=23)],history_since=0,ephemeral='retain'),
    dict(version=1,filters=[dict(kinds=[1])],history_since=0,ephemeral='retain'))
UPPERS=((86400,24),(86400,24,2))
TURN_SCOPES=(0,0,1,1,1)
PAGES=((0,1),(1,),(0,1),(1,2),(2,))


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
    assert [event['created_at'] for event in events]==[24,23,1]
    assert len({event['id'] for event in events})==3
    assert all(event['kind']==1 and len(bytes.fromhex(event['sig']))==64 for event in events)
    return events


def relay(directory,listener,finished,errors):
    number=0;nonces=set()
    try:
        assert json.loads((directory/'linear-capped-profile.json').read_text())==PROFILE
        events=corpus(directory)
        while not finished.is_set():
            try:connection,remote=listener.accept()
            except socket.timeout:continue
            number+=1
            with connection:
                assert number<=5,'bounded continuation opened a sixth socket'
                assert remote[0]=='127.0.0.1'
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
                scope=TURN_SCOPES[number-1];turn=TURN_SCOPES[:number].count(scope)
                upper=UPPERS[scope][turn-1]
                assert opcode==1 and len(request)==3 and request[0]=='REQ'
                assert request[2]==dict(kinds=[1],since=23 if scope==0 else 0,until=upper-1,limit=3)
                ident=request[1]
                assert re.fullmatch('[0-9a-f]{48}',ident) and int(ident[:32],16) and ident[:32] not in nonces
                nonces.add(ident[:32])
                values=[events[index] for index in PAGES[number-1]]
                assert 0<len(values)<=2<request[2]['limit']
                trace=dict(number=number,scope_number=scope,scope_turn=turn,request=request,
                    connected_at_us=connected,request_at_us=time.time_ns()//1000,
                    clock_resolution_us=math.ceil(time.clock_getres(time.CLOCK_REALTIME)*1_000_000))
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
                if number==5:
                    publish(directory,'capped-peer-five-complete.json',dict(format=PROFILE['format'],requests=5,
                        listener_closed=False,listening=listener.getsockopt(socket.SOL_SOCKET,socket.SO_ACCEPTCONN)==1,
                        completed_at_us=time.time_ns()//1000))
    except BaseException as error:
        errors.append(type(error).__name__+': '+str(error));publish(directory,'relay-error.json',dict(error=errors[-1]))
    finally:
        listener.close()
        publish(directory,'capped-peer-joined.json',dict(format=PROFILE['format'],requests=number,
            listener_closed=True,joined_at_us=time.time_ns()//1000,errors=len(errors)))
