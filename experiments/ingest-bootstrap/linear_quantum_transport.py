"""Finite current linear-quantum peer, private loopback only; no product proof."""
import base64
import hashlib
import json
import socket
import time
from installed_sync_client import exact,read_frame,send_frame,publish

PROFILE=dict(format='bigbrotr-linear-quantum-fixture/1',requests=8,scopes=2)
SCENARIOS=('slow_eose','malformed_eose','frame_cap_eose','event_cap_eose','closed','deadline','empty','proof')


def relay(directory,listener,finished,errors):
    """Every request uses the real handshake and actual generated nonce."""
    try:
        assert json.loads((directory/'linear-quantum-profile.json').read_text())==PROFILE
        event=json.loads((directory/'event.jsonl').read_text())
        fault=json.loads((directory/'linear-quantum-fault.json').read_text()) if (directory/'linear-quantum-fault.json').exists() else None
        scenarios=('body_closed',*SCENARIOS[1:]) if fault=='gap-write' else ('empty',*SCENARIOS[1:]) if fault=='empty-terminal-authority' else SCENARIOS
        for number,scenario in enumerate(scenarios,1):
            while True:
                try:connection,remote=listener.accept();break
                except socket.timeout:
                    if finished.is_set():return
            assert remote[0]=='127.0.0.1'
            with connection:
                connection.settimeout(3)
                header=bytearray();connected=time.time_ns()//1000
                while not header.endswith(b'\r\n\r\n'):
                    assert len(header)<4096;header.extend(exact(connection,1))
                lines=header.decode('ascii').split('\r\n');fields={}
                assert lines[0]=='GET /installed-fixture HTTP/1.1'
                for line in lines[1:]:
                    if line:
                        key,value=line.split(':',1);assert key.lower() not in fields;fields[key.lower()]=value.strip()
                assert fields['host']=='127.0.0.1:7447' and fields['upgrade'].lower()=='websocket'
                key=fields['sec-websocket-key'];assert len(base64.b64decode(key,validate=True))==16
                accept=base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest())
                connection.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+b'\r\n\r\n')
                opcode,encoded=read_frame(connection);request=json.loads(encoded)
                assert opcode==1 and len(request)==3 and request[0]=='REQ'
                assert request[2]==dict(kinds=[1],since=0,until=86399,limit=3)
                ident=request[1];assert len(ident)==48 and int(ident,16)
                trace=dict(number=number,scenario=scenario,request=request,connected_at_us=connected,
                    request_at_us=time.time_ns()//1000,clock_resolution_us=1,frames=[])
                publish(directory,f'relay-open-{number:03}.json',trace)
                # The first deliberately slow complete EVENT precedes EOSE.
                frames=([['EVENT',ident,event],['EOSE',ident]] if scenario in ('slow_eose','proof') else
                    [['EVENT',ident,{'malformed':True}],['EVENT',ident,event],['EOSE',ident]] if scenario=='malformed_eose' else
                    [['NOTICE','bounded frame']]*33+[['EOSE',ident]] if scenario=='frame_cap_eose' else
                    [['EVENT',ident,event]]*4+[['EOSE',ident]] if scenario=='event_cap_eose' else
                    [['EVENT',ident,event],['CLOSED',ident,'rate-limited']] if scenario=='body_closed' else
                    [['CLOSED',ident,'rate-limited']] if scenario=='closed' else
                    [['EOSE',ident]] if scenario=='empty' else [])
                trace['sent_at_us']=time.time_ns()//1000
                for index,frame in enumerate(frames):
                    try:send_frame(connection,frame)
                    except (BrokenPipeError,ConnectionResetError):break
                    trace['frames'].append(frame[0])
                    if frame[0] in ('EOSE','CLOSED'):trace['terminal_frame_hex']=json.dumps(frame,separators=(',',':')).encode().hex()
                    if scenario=='slow_eose' and index==0:
                        publish(directory,'slow-event-sent.json',dict(number=number))
                        time.sleep(.05)
                teardown=dict(request_close=False,websocket_close=False,ack_sent=False,eof=False)
                capped=scenario in ('frame_cap_eose','event_cap_eose')
                if capped:
                    # All cap+later-EOSE frames were emitted. Deliberately omit a
                    # WebSocket close ACK: the actual teardown must fail.
                    connection.shutdown(socket.SHUT_WR)
                if scenario!='deadline':
                    try:
                        opcode,raw=read_frame(connection)
                        if scenario not in ('closed','body_closed'):
                            assert opcode==1 and json.loads(raw)==['CLOSE',ident]
                            teardown['request_close']=True
                            opcode,raw=read_frame(connection)
                        assert opcode==8 and raw==b'';teardown['websocket_close']=True
                        if not capped:
                            connection.sendall(b'\x88\x00');connection.shutdown(socket.SHUT_WR)
                            teardown['ack_sent']=True
                    except (EOFError,BrokenPipeError,ConnectionResetError):
                        assert capped
                try:assert connection.recv(1)==b''
                except ConnectionResetError:assert capped
                teardown['eof']=True
                trace.update(closed_at_us=time.time_ns()//1000,teardown=teardown,
                    completion='request-and-websocket-close' if teardown['ack_sent'] else 'interrupted-socket-close')
                publish(directory,f'relay-closed-{number:03}.json',trace)
        publish(directory,'linear-peer-complete.json',dict(format=PROFILE['format'],requests=8,listener_closed=True))
    except BaseException as error:
        errors.append(type(error).__name__+': '+str(error));publish(directory,'relay-error.json',dict(error=errors[-1]))
    finally:listener.close()
