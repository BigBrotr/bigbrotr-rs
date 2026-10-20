#!/usr/bin/env python3
"""Frozen signed EVENT bytes and bounded server WebSocket framing.

Qualification input encoder only: no sockets, signing, SQL, authority or producer
state. A controlled relay calls send_event on its actual accepted connection.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

RECIPE_SHA256 = '815bf9e7e0fbcda73a77fbc3f91bebe0ec5c5f356e01bd665f695a8edb5af963'
EVENT_SHA256 = '5f880ef37e4bdafb8dd37c42a796afa5c4db1a2f9a5363b625c3bbaba2b78727'
EVENT_ID = '05e909d2c260edbb52e21e3813a492ade70523c8493fb0ae5a9a37a04354db31'
EVENT_BYTES = 94577
MAX_TEXT_BYTES = 128 * 1024
FIELDS = ('pubkey', 'created_at', 'kind', 'tags', 'content', 'id', 'sig')


def require(ok, label):
    if not ok:
        raise ValueError(label)


def encoded(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()


def signed_event(recipe):
    with Path(recipe).open('rb') as stream:
        raw = stream.read(16385)
    require(len(raw) <= 16384 and hashlib.sha256(raw).hexdigest() == RECIPE_SHA256,
            'exact independent frozen recipe')
    value = json.loads(raw)
    require(value['serialization_fields'] == list(FIELDS)
            and value['content_count'] == 4096, 'frozen recipe framing')
    event = dict(value['event'], content=value['content_repeat'] * value['content_count'])
    require(set(event) == set(FIELDS), 'complete original signed fields')
    body = encoded({key: event[key] for key in FIELDS})
    require(len(body) == EVENT_BYTES == value['signed_event_bytes']
            and hashlib.sha256(body).hexdigest() == EVENT_SHA256 == value['signed_event_sha256'],
            'full original signed bytes')
    require(event['id'] == EVENT_ID == hashlib.sha256(encoded([
        0, event['pubkey'], event['created_at'], event['kind'], event['tags'], event['content']
    ])).hexdigest(), 'independent canonical event ID')
    return body


def server_text_frame(body):
    """FIN/text, unmasked server frame; shortest length encoding, fixed128KiB cap."""
    require(type(body) is bytes and 0 < len(body) <= MAX_TEXT_BYTES, 'finite text bound')
    body.decode('utf-8')
    size = len(body)
    if size < 126:
        length = bytes([size])
    elif size < 65536:
        length = b'\x7e' + size.to_bytes(2, 'big')
    else:
        length = b'\x7f' + size.to_bytes(8, 'big')
    return b'\x81' + length + body


def event_frame(subscription, body):
    require(type(subscription) is str and re.fullmatch('[0-9a-f]{48}', subscription)
            and int(subscription[:32], 16) != 0, 'actual finite subscription ID')
    require(type(body) is bytes and len(body) == EVENT_BYTES
            and hashlib.sha256(body).hexdigest() == EVENT_SHA256, 'frozen event bytes')
    # Preserve field order and every byte of the independently signed event.
    return server_text_frame(b'["EVENT",' + encoded(subscription) + b',' + body + b']')


def send_event(connection, subscription, body):
    """Use only inside the relay's existing handshake/3s socket/custody lifetime."""
    require(connection.gettimeout() is not None and 0 < connection.gettimeout() <= 3,
            'relay must own a finite socket timeout')
    frame = event_frame(subscription, body)
    connection.sendall(frame)
    return dict(event_id=EVENT_ID, signed_bytes=EVENT_BYTES, signed_sha256=EVENT_SHA256,
                frame_bytes=len(frame), frame_sha256=hashlib.sha256(frame).hexdigest(),
                length_encoding=127)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('recipe', type=Path)
    parser.add_argument('event_output', type=Path)
    args = parser.parse_args()
    body = signed_event(args.recipe)
    # No newline: this artifact is the exact frozen signed event byte sequence.
    with args.event_output.open('xb') as stream:
        stream.write(body)
    print(json.dumps(dict(event_id=EVENT_ID, bytes=EVENT_BYTES, sha256=EVENT_SHA256)))


if __name__ == '__main__':
    main()
