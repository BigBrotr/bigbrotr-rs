"""Disposable RocksDB atomic admission / process-death experiment, not product code."""
import json
import multiprocessing as mp
import os
from pathlib import Path
import signal
import time
from importlib.metadata import version
from rocksdict import Rdict, Options, WriteBatch, WriteOptions

ROOT = Path(os.environ['LAB_ROOT']) / 'rocks-admission'

def db(path):
    opts = Options(raw_mode=True)
    opts.create_if_missing(True)
    opts.set_write_buffer_size(4 * 1024 * 1024)
    opts.set_max_background_jobs(1)
    return Rdict(str(path), opts)

def writer(path, committed, pipe):
    d = db(path)
    batch = WriteBatch(raw_mode=True)
    for key, value in [(b'identity:event', b'1'), (b'body:event', b'full-signed-fixture'),
                       (b'outbox:1', b'event'), (b'request:one', b'1'), (b'next-sequence', b'2')]:
        batch.put(key, value)
    if committed:
        options = WriteOptions(); options.sync = True; options.disable_wal = False
        d.write(batch, options)
    pipe.send('after_sync_write' if committed else 'before_write')
    time.sleep(30)

def run(n, committed):
    path = ROOT / str(n)
    parent, child = mp.Pipe(False)
    p = mp.Process(target=writer, args=(path, committed, child)); p.start()
    if not parent.poll(20):
        p.kill(); p.join(); raise RuntimeError('writer timeout')
    phase = parent.recv()
    os.kill(p.pid, signal.SIGKILL); p.join(5)
    assert p.exitcode == -signal.SIGKILL
    d = db(path)
    values = [d.get(k) for k in [b'identity:event', b'body:event', b'outbox:1', b'request:one', b'next-sequence']]
    assert all(v is not None for v in values) if committed else all(v is None for v in values)
    exclusive = False
    try:
        other = db(path)
    except Exception:
        exclusive = True
    else:
        other.close()
    assert exclusive
    d.close()
    return {'phase':phase, 'keys_present':sum(v is not None for v in values), 'second_owner_refused':exclusive}

if __name__ == '__main__':
    mp.set_start_method('spawn'); ROOT.mkdir(parents=True, exist_ok=True)
    report={'binding':'rocksdict '+version('rocksdict'),'scope':'synchronized atomic batch before/after process SIGKILL; no power-loss or disk-error injection; Python binding, not final Rust adapter',
            'cases':[run(0,False),run(1,True)],'status':'passed'}
    (ROOT.parent/'results'/'rocks-admission.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
