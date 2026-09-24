"""Discriminate the second actual authorization Execute in the wire fixture."""
from pathlib import Path
import sys
import unittest
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'archive-engine'))
from native_commit_proxy import ResumeAuthorityObserver


class ResumeAuthorityTests(unittest.TestCase):
    def parse(self, o, name=b's', query=None):
        return o.frontend(b'P', name+b'\0'+(query or o.QUERY)+b'\0\0\0')

    def bind(self, o, name=b's'):
        return o.frontend(b'B', b'\0'+name+b'\0'+b'\0'*6)

    def test_second_execute_only_including_statement_reuse(self):
        o=ResumeAuthorityObserver()
        self.assertFalse(self.parse(o)); self.assertFalse(self.bind(o))
        self.assertFalse(o.frontend(b'E',b'\0'*5)); self.assertEqual(o.requests,1)
        self.bind(o); self.assertTrue(o.frontend(b'E',b'\0'*5))
        self.bind(o); self.assertFalse(o.frontend(b'E',b'\0'*5)); self.assertEqual(o.requests,3)

    def test_preparation_and_other_queries_do_not_count(self):
        o=ResumeAuthorityObserver(); self.parse(o)
        self.parse(o,b'other',b'SELECT 1'); self.bind(o,b'other')
        self.assertFalse(o.frontend(b'E',b'\0'*5)); self.assertEqual(o.requests,0)
        self.bind(o);self.assertFalse(o.frontend(b'E',b'\0'*5));self.assertEqual(o.requests,1)

    def test_replaced_and_closed_statements_are_not_selected(self):
        for close in [False,True]:
            o=ResumeAuthorityObserver();self.parse(o)
            if close:o.frontend(b'C',b'Ss\0')
            else:self.parse(o,query=b'SELECT 1')
            self.bind(o);self.assertFalse(o.frontend(b'E',b'\0'*5));self.assertEqual(o.requests,0)

    def test_named_portal_and_row_limit_refuse_unknown_profiles(self):
        for tag,body in [(b'B',b'p\0s\0'+b'\0'*6),(b'E',b'\0\0\0\0\1')]:
            with self.assertRaises(AssertionError):ResumeAuthorityObserver().frontend(tag,body)


class ResumeWireTests(unittest.IsolatedAsyncioTestCase):
    async def test_second_execute_is_held_before_upstream_receives_it(self):
        import asyncio
        import struct
        from unittest.mock import patch
        import native_commit_proxy as module
        proxy=module.Proxy('hold-resume-authority','control-export-resume')
        held=asyncio.Event();second_bound=asyncio.Event();executed=[];tasks=[]
        proxy.emit=lambda event,**fields:held.set() if event=='held' else None
        def frame(tag,body):return tag+struct.pack('!I',len(body)+4)+body
        async def upstream(reader,writer):
            try:
                n=struct.unpack('!I',await reader.readexactly(4))[0];await reader.readexactly(n-4)
                writer.write(frame(b'K',struct.pack('!II',123,0))+frame(b'Z',b'I'));await writer.drain()
                for number in range(2):
                    self.assertEqual((await proxy.frame(reader))[0],b'P')
                    self.assertEqual((await proxy.frame(reader))[0],b'S')
                    writer.write(frame(b'1',b'')+frame(b'Z',b'T'));await writer.drain()
                    self.assertEqual((await proxy.frame(reader))[0],b'B')
                    if number==1:second_bound.set()
                    self.assertEqual((await proxy.frame(reader))[0],b'E');executed.append(number)
                    self.assertEqual((await proxy.frame(reader))[0],b'S')
                    writer.write(frame(b'2',b'')+frame(b'C',b'SELECT 1\0')+frame(b'Z',b'T'));await writer.drain()
                await reader.read()
            finally:writer.close();await writer.wait_closed()
        def spawn(coroutine):tasks.append(asyncio.create_task(coroutine))
        backend=await asyncio.start_server(lambda r,w:spawn(upstream(r,w)),'127.0.0.1',0)
        listener=await asyncio.start_server(lambda r,w:spawn(proxy.session(r,w)),'127.0.0.1',0)
        connect=asyncio.open_connection;writer=None
        async def redirect(host,port,*args,**kwargs):
            return await connect(host,backend.sockets[0].getsockname()[1] if port==5432 else port,*args,**kwargs)
        try:
            with patch.object(module.asyncio,'open_connection',redirect):
                reader,writer=await connect('127.0.0.1',listener.sockets[0].getsockname()[1])
                startup=b'\0\3\0\0user\0bigbrotr_control\0database\0bbpub_control_normal\0\0'
                writer.write(struct.pack('!I',len(startup)+4)+startup);await writer.drain()
                self.assertEqual((await proxy.frame(reader))[0],b'K');self.assertEqual((await proxy.frame(reader))[0],b'Z')
                for number in range(2):
                    name=str(number).encode()
                    writer.write(frame(b'P',name+b'\0'+ResumeAuthorityObserver.QUERY+b'\0\0\0')+frame(b'S',b''));await writer.drain()
                    self.assertEqual((await proxy.frame(reader))[0],b'1');self.assertEqual((await proxy.frame(reader))[0],b'Z')
                    writer.write(frame(b'B',b'\0'+name+b'\0'+b'\0'*6)+frame(b'E',b'\0'*5)+frame(b'S',b''));await writer.drain()
                    if number==1:
                        await asyncio.wait_for(held.wait(),1);await asyncio.wait_for(second_bound.wait(),1)
                        self.assertEqual(executed,[0]);self.assertEqual(proxy.report['resume_authority_requests'],2)
                        proxy.action='release';proxy.release.set()
                    for tag in [b'2',b'C',b'Z']:self.assertEqual((await proxy.frame(reader))[0],tag)
                writer.close();await writer.wait_closed();await asyncio.wait_for(asyncio.gather(*tasks),2)
                self.assertEqual(executed,[0,1]);self.assertTrue(proxy.held)
        finally:
            if writer:writer.close();await writer.wait_closed()
            listener.close();backend.close();await listener.wait_closed();await backend.wait_closed()
            for task in tasks:task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)


if __name__=='__main__':unittest.main()
