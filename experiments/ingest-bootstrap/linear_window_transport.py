"""Declared populated islands, empty spans and same-second ID progression."""
import hashlib
import errno
import json
import socket
import time

from installed_sync_client import publish
from linear_capped_transport import accept_nowait, exchange_page, IMPORT_SHA256, WINDOW_SHA256


PROFILE = dict(format='bigbrotr-linear-window-fixture/1', requests=8, scopes=1)
SELECTOR = dict(version=1, filters=[dict(kinds=[1], until=259199)],
                history_since=0, ephemeral='retain')
ENTRIES = (('request', 0, 86400, 86400), ('request', 0, 2, 86400),
           ('request', 0, 1, 86400), ('request', 86400, 172800, 172800),
           ('request', 172800, 259200, 259200), ('request', 172800, 172802, 259200),
           ('request', 172800, 172802, 259200), ('request', 172800, 172801, 259200))
PAGES = ((0,), (0,), (), (), (1,), (2,), (2,), ())


def corpus(directory):
    original = (directory / 'event.jsonl').read_bytes()
    window = (directory / 'linear-window-events.json').read_bytes()
    assert hashlib.sha256(original).hexdigest() == IMPORT_SHA256
    assert hashlib.sha256(window).hexdigest() == WINDOW_SHA256
    fixture = json.loads(window)
    assert fixture['format'] == 'bigbrotr-linear-window-events/1'
    events = [json.loads(original)]
    for name in ('island-first', 'island-second'):
        row, = [row for row in fixture['events'] if row['name'] == name]
        event = row['event']
        canonical = json.dumps([0, event['pubkey'], event['created_at'], event['kind'],
            event['tags'], event['content']], ensure_ascii=False, separators=(',', ':')).encode()
        assert canonical.hex() == row['canonical_hex']
        assert hashlib.sha256(canonical).hexdigest() == event['id']
        events.append(event)
    assert [event['created_at'] for event in events] == [1, 172801, 172801]
    assert all(event['kind'] == 1 and event['tags'] == [] for event in events)
    assert len({event['id'] for event in events}) == 3
    return events


def relay(directory, listener, finished, errors):
    number, nonces, completed = 0, set(), False
    try:
        assert json.loads((directory / 'linear-window-profile.json').read_text()) == PROFILE
        events = corpus(directory)
        while not finished.is_set():
            probe = number == 8 and not completed
            try:
                connection, remote = accept_nowait(listener) if probe else listener.accept()
            except BlockingIOError as error:
                if not probe or error.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise
                if finished.is_set():
                    continue
                # Prior accepts and source EOF belong to this same listener.
                # Probe kernel accept directly; Darwin cannot query SO_ACCEPTCONN.
                listening = listener.fileno() >= 0
                assert listening, 'window listener closed before completion'
                publish(directory, 'linear-window-peer-complete.json', dict(
                    format=PROFILE['format'], requests=8, listener_closed=False,
                    listening=listening, completed_at_us=time.time_ns() // 1000))
                completed = True
                continue
            except socket.timeout:
                if probe:
                    raise
                continue
            number += 1
            with connection:
                assert number <= 8, 'settled pass opened a ninth socket'
                assert remote[0] == '127.0.0.1'
                _, lower, upper, _ = ENTRIES[number - 1]
                exchange_page(directory, connection, number,
                    dict(kinds=[1], since=lower, until=upper - 1, limit=3),
                    [events[index] for index in PAGES[number - 1]], nonces)
    except BaseException as error:
        errors.append(type(error).__name__ + ': ' + str(error))
        publish(directory, 'relay-error.json', dict(error=errors[-1]))
    finally:
        listener.close()
        publish(directory, 'linear-window-peer-joined.json', dict(
            format=PROFILE['format'], requests=number, listener_closed=True,
            joined_at_us=time.time_ns() // 1000, errors=len(errors)))
