#!/usr/bin/env python3
"""Independent fixed-width visibility request vector; no product code imports."""
import hashlib
import json
from pathlib import Path
import struct

read = dict(dataset='01'*16, scope=dict(dataset_id='01'*16, dictionary_kind=4,
            allocation_domain='02'*16, ordinal='1'), topology_generation='0',
            bounds=[dict(shard_id=0, owner_epoch=1, complete_through=1)], snapshot_id=2)
preimage = (b'bigbrotr/archive-visibility-read/1\0' + bytes([1])*32
            + struct.pack('>H', 4) + bytes([2])*16
            + struct.pack('>QQQQIQQ', 1, 0, 2, 1, 0, 1, 1))
value = dict(format='independent-archive-visibility/1', read=read,
             preimage_hex=preimage.hex(), request_digest=hashlib.sha256(preimage).hexdigest())
Path(__file__).with_name('archive-visibility.json').write_text(json.dumps(value, indent=2)+'\n')
