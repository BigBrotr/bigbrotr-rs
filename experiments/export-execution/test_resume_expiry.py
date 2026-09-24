"""Hold the actual resume COMMIT and recognize only its exact expiry refusal."""
import asyncio
from pathlib import Path
import struct
import sys
import unittest
from unittest.mock import patch
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'archive-engine'))
import native_commit_proxy as module


class CommitExpiryWireTests(unittest.IsolatedAsyncioTestCase):
    async def exchange(self, rejection):
        proxy=module.Proxy('hold-commit','control-export-resume')
        held=asyncio.Event();updated=asyncio.Event();commits=[];tasks=[]
        proxy.emit=lambda event,**fields:held.set() if event=='held' else None
        def frame(tag,body):return tag+struct.pack('!I',len(body)+4)+body
        async def backend(reader,writer):
            try:
                size=struct.unpack('!I',await reader.readexactly(4))[0]
                await reader.readexactly(size-4)
                writer.write(frame(b'K',struct.pack('!II',321,0))+frame(b'Z',b'I'));await writer.drain()
                self.assertEqual((await proxy.frame(reader))[2],b'BEGIN\0')
                writer.write(frame(b'C',b'BEGIN\0')+frame(b'Z',b'T'));await writer.drain()
                self.assertEqual((await proxy.frame(reader))[0],b'P')
                self.assertEqual((await proxy.frame(reader))[0],b'S')
                writer.write(frame(b'1',b'')+frame(b'Z',b'T'));await writer.drain()
                self.assertEqual((await proxy.frame(reader))[0],b'B')
                self.assertEqual((await proxy.frame(reader))[0],b'E')
                self.assertEqual((await proxy.frame(reader))[0],b'S')
                writer.write(frame(b'2',b'')+frame(b'C',b'INSERT 0 1\0')+frame(b'Z',b'T'));await writer.drain()
                self.assertEqual((await proxy.frame(reader))[2],b'UPDATE control.jobs SET state=\'queued\'\0')
                writer.write(frame(b'C',b'UPDATE 1\0')+frame(b'Z',b'T'));await writer.drain();updated.set()
                commit=await proxy.frame(reader);self.assertEqual(commit[2],b'COMMIT\0');commits.append(True)
                if rejection:
                    code,message=rejection
                    response=frame(b'E',b'SERROR\0C'+code+b'\0M'+message+b'\0\0')
                else:response=frame(b'C',b'COMMIT\0')
                writer.write(response+frame(b'Z',b'I'));await writer.drain()
                # A known rejection ends the transaction, not this connection.
                self.assertEqual((await proxy.frame(reader))[2],b'SELECT 1\0')
                writer.write(frame(b'C',b'SELECT 1\0')+frame(b'Z',b'I'));await writer.drain()
                await reader.read()
            finally:writer.close();await writer.wait_closed()
        def spawn(coroutine):tasks.append(asyncio.create_task(coroutine))
        server=await asyncio.start_server(lambda r,w:spawn(backend(r,w)),'127.0.0.1',0)
        listener=await asyncio.start_server(lambda r,w:spawn(proxy.session(r,w)),'127.0.0.1',0)
        connect=asyncio.open_connection;writer=None
        async def redirect(host,port,*args,**kwargs):
            return await connect(host,server.sockets[0].getsockname()[1] if port==5432 else port,*args,**kwargs)
        try:
            with patch.object(module.asyncio,'open_connection',redirect):
                reader,writer=await connect('127.0.0.1',listener.sockets[0].getsockname()[1])
                async def receive(*tags):
                    for tag in tags:self.assertEqual((await proxy.frame(reader))[0],tag)
                async def send(*parts):writer.write(b''.join(parts));await writer.drain()
                startup=b'\0\3\0\0user\0bigbrotr_control\0database\0bbpub_control_normal\0\0'
                await send(struct.pack('!I',len(startup)+4)+startup);await receive(b'K',b'Z')
                await send(frame(b'Q',b'BEGIN\0'));await receive(b'C',b'Z')
                await send(frame(b'P',b's\0INSERT INTO control.export_resume_commands(actor_id) VALUES ($1)\0\0\0'),frame(b'S',b''));await receive(b'1',b'Z')
                await send(frame(b'B',b'\0s\0'+b'\0'*6),frame(b'E',b'\0'*5),frame(b'S',b''));await receive(b'2',b'C',b'Z')
                await send(frame(b'Q',b"UPDATE control.jobs SET state='queued'\0"));await receive(b'C',b'Z')
                await send(frame(b'Q',b'COMMIT\0'))
                await asyncio.wait_for(held.wait(),1);await asyncio.wait_for(updated.wait(),1)
                self.assertEqual(commits,[])
                self.assertEqual(proxy.report['data_queries'],1);self.assertEqual(proxy.report['data_commits'],1)
                self.assertEqual(proxy.report['server_complete'],0);self.assertEqual(proxy.report['client_complete'],0)
                proxy.action='release';proxy.release.set()
                await receive(b'E' if rejection else b'C',b'Z')
                await send(frame(b'Q',b'SELECT 1\0'));await receive(b'C',b'Z')
                writer.close();await writer.wait_closed();await asyncio.wait_for(asyncio.gather(*tasks),2)
                self.assertEqual(commits,[True])
                expected=int(rejection==(b'BB001',b'export resume session authority expired or changed before commit'))
                self.assertEqual(proxy.report['resume_expiry_rejections'],expected)
                self.assertEqual(proxy.report['resume_expiry_idle'],expected)
                self.assertEqual(proxy.report['server_complete'],int(rejection is None))
                self.assertEqual(proxy.report['client_complete'],int(rejection is None))
                self.assertEqual(proxy.report['rejection_sequence'],['other'] if rejection else [])
        finally:
            if writer:writer.close();await writer.wait_closed()
            listener.close();server.close();await listener.wait_closed();await server.wait_closed()
            for task in tasks:task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)

    async def test_expired_commit_is_held_then_rejected_on_reusable_connection(self):
        await self.exchange((b'BB001',b'export resume session authority expired or changed before commit'))

    async def test_live_commit_is_held_then_completed(self):
        await self.exchange(None)

    async def test_other_code_or_message_is_not_expiry_evidence(self):
        for rejection in [(b'23514',b'export resume session authority expired or changed before commit'),(b'BB001',b'current grants refused')]:
            with self.subTest(rejection=rejection):await self.exchange(rejection)


if __name__=='__main__':unittest.main()
