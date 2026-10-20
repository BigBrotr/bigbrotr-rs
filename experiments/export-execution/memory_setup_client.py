"""Actual private HTTP commands and explicit frozen large-frame relay callback."""
import http.client
import json
from pathlib import Path
import threading

from memory_setup_io import require, sha, write
from memory_relay import event_frame, send_event
from installed_sync_client import relay, relay_listener

ORIGIN = 'http://127.0.0.1:18081'
ACTIONS = ('history.read', 'export', 'private-export')


class Client:
    def __init__(self, io, stage, principal):
        self.io, self.stage, self.principal = io, stage, principal
        self.serial = 0
        self.session_path = io.root / (stage + '-session.private')
        self.login = io.cli(stage + '-login', 'access', 'login', '--origin', ORIGIN,
            '--username', io.spec['username'], '--password-ref', io.spec['password_ref'],
            '--session-output', str(self.session_path))
        saved = json.loads(self.session_path.read_text())
        require(saved['format'] == 'bigbrotr-cli-session/1' and saved['origin'] == ORIGIN
                and self.session_path.stat().st_mode & 0o777 == 0o600, 'actual protected session')
        self.auth = {'Cookie': 'BigBrotrLocalSession=' + saved['session'], 'X-CSRF-Token': saved['csrf']}
        code, self.me = self.request('GET', '/management/v1/me')
        require(code == 200 and self.me['principal_id'] == principal, 'actual enrolled principal')
        require({'data.read', 'policy.change', 'principals.manage', 'roles.control', 'status.read'}
                <= set(self.me['stored_actions']), 'explicit enrolled actions')

    def request(self, method, path, body=None, key=None):
        connection = http.client.HTTPConnection('127.0.0.1', 18081, timeout=3)
        try:
            connection.request(method, path, None if body is None else json.dumps(body),
                {'Origin': ORIGIN, 'Content-Type': 'application/json', **self.auth,
                 **({'Idempotency-Key': key} if key else {})})
            response = connection.getresponse()
            raw = response.read(65537)
            require(len(raw) <= 65536, 'bounded management response')
            value = json.loads(raw)
            self.serial += 1
            write(self.io.root / f'{self.stage}-http-{self.serial:02}.json',
                dict(method=method, path=path, key=key, body=None if body is None else
                     {k: v for k, v in body.items() if k != 'password'},
                     status=response.status, response=value))
            return response.status, value
        finally:
            connection.close()

    def create(self, resource, command, key):
        status, value = self.request('POST', '/management/v1/' + resource, command, key)
        require(status == 202 and value['created'] and value['command_key'] == key,
                'known actual source command receipt; never retry uncertainty')
        return value

    def acquire(self, scope, revisions):
        keys = self.io.spec['keys']
        endpoint = self.create('relay-endpoints', dict(
            url='WS://127.0.0.1:7447/a/../installed-fixture', **revisions), keys['endpoint'])
        enrollment = self.create('relay-enrollments', dict(endpoint_id=endpoint['endpoint_id'],
            visibility=scope, route=dict(**{'class': 'private'}, cidr='127.0.0.1/32'),
            enabled=True, expected_generation=None, **revisions), keys['enrollment'])
        selected = self.create('acquisition-scopes', dict(endpoint_id=endpoint['endpoint_id'],
            access_scope_id=enrollment['access_scope_id'], visibility=scope,
            selector=dict(version=1, filters=[{'kinds': [1]}], history_since=0, ephemeral='retain'),
            **revisions), keys['scope'])
        receipt = self.create('jobs', dict(kind='acquisition', scope_id=selected['scope_id'],
            deadline_us='120000000', **revisions), keys['acquisition'])
        require(receipt['execution'] == 'unobserved' and receipt['authority'] == 'acceptance-only',
                'source acceptance has no native authority')
        result = dict(endpoint=endpoint, enrollment=enrollment, scope=selected, acquisition=receipt)
        write(self.io.root / 'acquisition-receipts.json', result)
        return result

    def grant(self, scope, allocation, revisions, account_revision):
        """This ordinary Serve client never submits an Export job."""
        receipts = []
        password = Path(self.io.spec['password_ref'].removeprefix('file:')).read_text()
        for index, action in enumerate(ACTIONS):
            command = dict(principal_id=self.principal, action=action, enabled=True,
                scope=scope, allocation=allocation, expected_grant_revision=str(account_revision + index),
                **revisions)
            key = self.io.spec['keys'][action]
            status, proof = self.request('POST', '/management/v1/sessions/current/reauthenticate',
                dict(password=password, command=command), key)
            require(status == 201, 'real action-bound reauthentication')
            status, receipt = self.request('PUT', '/management/v1/principals/' + self.principal + '/grants',
                dict(command=command, proof_id=proof['proof_id']), key)
            require(status == 200 and receipt['created'] and receipt['scope_generation'] == '1'
                    and receipt['account_grant_revision'] == str(account_revision + index + 1),
                    'actual explicit scope grant')
            receipts.append(dict(command=command, key=key, proof=proof, receipt=receipt))
        del password
        write(self.io.root / 'scope-grant-receipts.json', receipts)
        return receipts

    def logout(self):
        self.io.cli(self.stage + '-logout', 'access', 'logout',
                    '--session-ref', 'file:' + str(self.session_path))
        require(self.request('GET', '/management/v1/me')[0] == 401, 'session actually revoked')
        self.auth = {}


class Peer:
    def __init__(self, root, body, *, kind='frozen-archive-recipe'):
        self.root = Path(root) / 'relay'
        self.root.mkdir(mode=0o700)
        # Existing handshake/REQ/CLOSE/3s socket owns the lifetime. The callback
        # alone handles the >65535-byte frame; the shared encoder is unchanged.
        (self.root / 'event.jsonl').write_bytes(body)
        self.finished, self.errors = threading.Event(), []

        def send(connection, subscription, number):
            trace = []
            for index, label in enumerate(('signed', 'duplicate')):
                frame = event_frame(subscription, body, kind=kind)
                path = self.root / f'frame-{number:03}-{index}.bin'
                with path.open('xb') as stream:
                    stream.write(frame)
                receipt = send_event(connection, subscription, body, kind=kind)
                require(receipt['frame_bytes'] == len(frame) and receipt['frame_sha256'] == sha(frame),
                        'complete transmitted frame binding')
                write(path.with_suffix('.json'), dict(subscription=subscription, label=label, **receipt))
                trace.append(label)
            return [*trace, 'EOSE']

        listener = relay_listener()
        self.thread = threading.Thread(target=relay,
            args=(self.root, listener, self.finished, self.errors, True),
            kwargs=dict(event_sender=send))
        self.thread.start()

    def joined(self):
        self.finished.set()
        self.thread.join(4)
        write(self.root / 'thread-join.json', dict(alive=self.thread.is_alive(), errors=self.errors))
        require(not self.thread.is_alive() and not self.errors, 'actual relay thread join')

    def transcript(self):
        require(not self.errors, 'relay error')
        return [json.loads(path.read_text()) for path in sorted(self.root.glob('relay-closed-*.json'))]
