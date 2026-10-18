"""Independent prepared-attempt expectations; no engine or product execution.

Literal endpoint pins bind qualified old science. The portable encoder and
lease reducer are reused from earlier independent readers. New expectations
cover exact gap completion, zero-request empty retirement and active Control
exclusion; no native/SQL scalar is treated as candidate authority.
"""
import copy
import re
import installed_cycle_model as cycle
import transition_replay as transition

history = cycle.history
only, same, sha, encoded, instant = history.only, history.same, history.sha, history.encoded, history.instant
binary, be = history.binary, history.be
native_map, native_rows = transition.native_map, transition.native_rows

PINS = {'terminal': {'name': 'terminal',
              'run': 'ingest-activated-retirement-engine-9bdc86a',
              'subdirectory': 'bindings',
              'database': 'bb_scope_bind_9bdc86a_installed_activated_retirement_off',
              'status': 'PASS_INSTALLED_ACTIVATED_RETIREMENT_FIRST_WAVE',
              'source': '9bdc86a2316e0aa0d1078ededb80a8d9c9502a2f',
              'owner': '1979cce97962b0239e79e3fa47b2e5ae',
              'label': 'org.bigbrotr.acquisition-binding',
              'client_id': 'fe56e36ac2dd47fa41842cef7d6bbc8564e2032352c5097f68448047bb2c89c0',
              'source_clone': 'installed-activated-retirement-off',
              'final_case': 'installed-activated-retirement-off-retry',
              'catalog_sha256': 'baa02a5fc187770cd8b5782ab47feab4dd11d54667aa2737d19411db546e9276',
              'result_sha256': '48506a152b393854f2f50cbedfedb71b66e53ed9578753d7e6b4df3bd76c8e09',
              'snapshot': 'installed-activated-retirement-off-retry-after-sql.json',
              'snapshot_sha256': '7ac690057de370a00144ffd415bb1f504b05ff4b5599d6458b728820a98acbad',
              'native_file': 'installed-activated-retirement-off-retry-after-native.json',
              'native_sha256': 'cd49f8bfc44c5c08ae307f845f1aaf1da7f66079bf14f8ee54f9c4c1a3819238',
              'inputs_file': 'installed-activated-retirement-off-retry-after-inputs.json',
              'inputs_sha256': 'f59224bde2f441efa1b74bbdb64c65ae5a30bdd2f283f9d31b5dd5c3adb82589',
              'store': 'installed-activated-retirement-off/store',
              'origin_format': 'legacy-installed',
              'verification_file': 'installed-activated-retirement-off-retry-verification.json',
              'verification_sha256': 'b1e0b4eaada03b9c169592750d70285e8234abfa7ebbebc61877255b3b046d82'},
 'open': {'name': 'open',
          'run': 'ingest-succession-transition-engine-9f06d15',
          'subdirectory': 'transition',
          'database': 'bb_transition_9f06d15_empty',
          'status': 'PASS_SUCCESSOR_TRANSITION_FIRST_WAVE',
          'source': '9f06d150eeec7613dd2d46c5bdc33ea574bf47d2',
          'owner': 'd332d7af276ab128cfd962a48dacd52f',
          'label': 'org.bigbrotr.succession-transition',
          'client_id': '898bc8d3f87b3c4e406ced9239b605c12067d67d4424a6a4de9739f4c891ee6b',
          'source_clone': 'empty',
          'final_case': 'empty-prepare4',
          'catalog_sha256': '062b2dcee8d28ce48461cca668dd3c9d20f94e3035041f1a817925fafa77263e',
          'result_sha256': 'c940d1a2a67746655ceefeade3e323802ac39960fb2e3d38325f204617ffcde3',
          'snapshot': 'empty-prepare4-after.json',
          'native_file': 'empty-prepare4-after-native.json',
          'inputs_file': 'empty-prepare4-after-inputs.json',
          'store': 'empty/store',
          'snapshot_sha256': 'f63c82e6c78466978b31ee4ff39a787ccda03cbed72d58a27167688003d374e5',
          'native_sha256': '9de597ad4849518ddd48d51ba2dd30d10c2b75518b38a266fac00cedffc613a1',
          'inputs_sha256': 'c757c9978a581ccef16c249f0fd1d8d15c64d9996b4c02b3bea1830a86cddba0'},
 'closing': {'name': 'closing',
             'run': 'ingest-succession-transition-engine-9f06d15',
             'subdirectory': 'transition',
             'database': 'bb_transition_9f06d15_activated',
             'status': 'PASS_SUCCESSOR_TRANSITION_FIRST_WAVE',
             'source': '9f06d150eeec7613dd2d46c5bdc33ea574bf47d2',
             'owner': 'd332d7af276ab128cfd962a48dacd52f',
             'label': 'org.bigbrotr.succession-transition',
             'client_id': '898bc8d3f87b3c4e406ced9239b605c12067d67d4424a6a4de9739f4c891ee6b',
             'source_clone': 'activated',
             'final_case': 'activated-prepare4',
             'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
             'result_sha256': 'c940d1a2a67746655ceefeade3e323802ac39960fb2e3d38325f204617ffcde3',
             'snapshot': 'activated-prepare4-after.json',
             'native_file': 'activated-prepare4-after-native.json',
             'inputs_file': 'activated-prepare4-after-inputs.json',
             'store': 'activated/store',
             'snapshot_sha256': '8c45b989bd6a5f2242a07544e859d92fda13ebf939af7fe6a79a55ff789d8bbe',
             'native_sha256': '732b3b8154713441792118c00dff876c1d64f6ee3277c9eebf51714ef53213d2',
             'inputs_sha256': '2a60c0a8f80eab6c2875f5546ab47df9053e2fac9f69b799fc7e1d276560af25'},
 'activated': {'name': 'activated',
               'run': 'ingest-succession-installed-cycle-engine-04e41c1',
               'subdirectory': 'installed-cycle',
               'database': 'bb_installed_cycle_04e41c1_activated',
               'status': 'PASS_INSTALLED_SUCCESSOR_CYCLE',
               'source': '04e41c162f9841a3378250252a46bda872d2de1f',
               'owner': 'ae7eae5475e07e4d07608b8acba5f5f4',
               'label': 'org.bigbrotr.installed-successor-cycle',
               'client_id': '961b9f088b73ab3c44ae492883e5ae6084db3d439adeab82d49570dacfc4eb99',
               'source_clone': 'activated',
               'final_case': 'activated-live',
               'catalog_sha256': '6aad32f83a7c62b071ec37852994df068afdcb7c334635b94aacf1d225a5718e',
               'result_sha256': 'ad63a79eb1b7d009cadbaa8dc06944e75fdbc98a261ee1d92881841c15a1bb54',
               'snapshot': 'activated-live-after.json',
               'native_file': 'activated-live-after-native.json',
               'inputs_file': 'activated-live-after-inputs.json',
               'store': 'activated/store',
               'snapshot_sha256': '9e1ddcd98e3adab92a42ff2b3e72eac9d8a0212449a578a87746398de87ff4e3',
               'native_sha256': 'cb50f14c535c7c24cbf9e92b21d09df0248664612a475feb07e011bb8fafbd3a',
               'inputs_sha256': '713ec780a8fc393ea136f16e8eb020ff019dd95d87c923f0b0030d28b375bc98'}}

# Hashes of canonical JSON, distinct from the retained file hashes above.
SEED_HASHES = {'activated': ['6d6ffdd9c8bce2f3f6b4baa86fc7eb0da36fdd831cb5a858dcd5097ac7a09a78',
               '825c426456f619c4f37119ea73a9f545be2bb59876286eb049f3c6d8459f4f29',
               'fedeae1f6ee5a679f33eb1b23a0b9130893cd38090cdcc8ef3957dfb270a0d5c'],
 'closing': ['8ef47bd5c9cd897723d6cb500ff46a6930c01a43266bbf191f73a71a38d4f030',
             'b94504b5a1d8670ba646aa2c04c7c3db9227f2bd8a4b7f1c78d58d1a13363875',
             '28bb113a743f8327830392c13fff29edfdcea38b66d82b332020d20c63a0618b'],
 'open': ['de80672008912b3f0a27e8b80bab7510f1c3546e1558aa1f810484de54dcd41b',
          'ae42a245375298c45727887a7b0fc74a6b030f8e6a1cfe8480aed39fc9bb3957',
          '5b925ffacfdebc27efde1c8b577757581692b4359272b708779a1b97bf82e7ea']}


def seed_checked(seed, origin, window):
    pg, native, inputs = seed
    assert len(pg) == 75
    if origin == 'terminal':
        history.baseline(pg, native, inputs, 'activated', window)
        history.science(pg, native, inputs, 'activated')
    else:
        assert [sha(encoded(history.normalized(pg))), sha(encoded(native)), sha(encoded(inputs))] == SEED_HASHES[origin]
    return max(r['producer_epoch'] for r in pg['producer_epochs'])


def coordinates(pg, native):
    legacy = only(pg['initial_ingest_successions'])
    values = native_map(native)
    head = int.from_bytes(bytes.fromhex(values['default', '0101'])[54:62], 'big')
    registered = max(r['producer_epoch'] for r in pg['producer_epochs'])
    assert registered in (2, 3, 4) and head in (2, 3, 4) and registered - head in (0, 1)
    return legacy, values, head, registered


def summary(pg, epoch):
    producer = only(pg['producer_epochs'], producer_epoch=epoch)
    source = only(pg['source_streams'], owner_epoch=epoch)
    assert producer['state'] == 'retired' and source['state'] == 'closed'
    if epoch == 1:
        count, cut, digest, kind, mask = 4, 12, history.CLOSURES[0], 'initial', 8
    elif epoch == 2 and pg['initial_ingest_activations']:
        count, cut, digest, kind, mask = 2, 5, history.CLOSURES[1], 'activated', 2
    elif epoch == 2:
        count, cut, kind, mask = 0, 0, 'unactivated-empty', 0
        legacy = only(pg['initial_ingest_successions'])
        # Legacy empty digest is pinned by the exact original receipt/science.
        digest = source['closure_digest'][2:]
        assert digest == history.CLOSURES[2]
    else:
        intent = only(pg['ingest_successions'], target_epoch=epoch)
        assert not any(r['target_epoch'] == epoch for r in pg['ingest_activations'])
        count, cut, kind, mask = 0, 0, 'unactivated-empty', 0
        digest = transition.empty_digest(binary(intent['handoff']), binary(intent['target_manifest']),
            binary(producer['producer_slot_id']), epoch)
    assert producer['reserved_through_request'] == count
    assert source['last_complete_sequence'] == cut
    assert producer['closure_digest'] == source['closure_digest'] == '\\x' + digest
    return dict(epoch=epoch, terminal_kind=kind, resolved_requests=count, last_complete_sequence=cut,
                digest=digest, event_request_mask=mask)


def complete_gap(native, intent):
    values = native_map(native)
    epoch = intent['target_epoch']
    assert values['default', '0101'] == intent['previous_manifest'][2:]
    for cf, key, value in (
        ('default', b'\x01\x01', binary(intent['target_manifest'])),
        ('default', b'\x01\x0a' + binary(intent['dataset_id']) + binary(intent['owner_resource_id']) + be(epoch-1), binary(intent['handoff'])),
        ('default', b'\x01\x02' + binary(intent['dataset_id']) + be(0,4) + be(epoch), b'\x01\x02' + be(0)),
        ('receipts', b'\x01\x31' + binary(intent['producer_slot_id']) + be(epoch), b'\x01\x31' + bytes(24))):
        assert key == b'\x01\x01' or (cf,key.hex()) not in values
        values[cf,key.hex()] = value.hex()
    native.update(rows=native_rows(values), sequence=native['sequence']+4)


def close(native, slot, epoch):
    values = native_map(native)
    key = 'receipts', (b'\x01\x31' + binary(slot) + be(epoch)).hex()
    opened, closed = (b'\x01\x31'+bytes(24)).hex(), (b'\x02\x31'+bytes(24)+b'\x01').hex()
    assert values[key] in (opened, closed)
    if values[key] == opened:
        values[key] = closed
        native.update(rows=native_rows(values), sequence=native['sequence']+1)


class Model(cycle.Model):
    def control_end(self, observed, *, released):
        """Actual final authority only; no invented intermediate refresh rows."""
        before = copy.deepcopy(self.pg)
        previous = max((r for r in self.pg['owner_epochs'] if r['role']=='control'),key=lambda r:r['epoch'])
        old = copy.deepcopy(previous)
        actual = only(observed['owner_epochs'],role='control',epoch=old['epoch']+1)
        role = only(observed['role_instances'],role_instance_id=actual['role_instance_id'])
        rid,inc = self.identity(role['role_instance_id'][2:]),self.identity(actual['process_incarnation'][2:])
        created,acquired,refreshed = role['created_at_us'],actual['acquired_at_us'],actual['lease_expires_at_us']-6_000_000
        for at in (created,acquired,refreshed): cycle.clock(at,self.window)
        assert created <= acquired <= refreshed
        assert type(actual['control_backend_pid']) is int and actual['control_backend_pid'] > 0
        assert self.window['before_us'] <= instant(actual['control_backend_start']) <= acquired
        assert (actual['control_backend_pid'],instant(actual['control_backend_start'])) != (old['control_backend_pid'],instant(old['control_backend_start']))
        cycle.fresh_xmin(actual['xmin'],before)
        if old['state']=='active':
            fenced = only(observed['owner_epochs'],role='control',epoch=old['epoch'])
            old_role = only(self.pg['role_instances'],role_instance_id=old['role_instance_id'])
            retired = only(observed['role_instances'],role_instance_id=old['role_instance_id'])
            for at in (fenced['released_at_us'],retired['retired_at_us']):
                cycle.clock(at,self.window); assert at <= acquired
            cycle.fresh_xmin(fenced['xmin'],before)
            assert fenced['xmin'] != actual['xmin']
            digest=sha(b'bigbrotr/control-exclusion/1\0'+binary(old['dataset_id'])+binary(old['resource_id'])
                +be(old['epoch'])+binary(old['process_incarnation'])+binary(inc))
            previous.update(state='released',released_at_us=fenced['released_at_us'],exclusion_method='storage_fenced',
                exclusion_digest='\\x'+digest,xmin=fenced['xmin'])
            old_role.update(retired_at_us=retired['retired_at_us'],xmin=fenced['xmin'])
        else:
            assert old['state']=='released'
        if released:
            for at in (actual['released_at_us'],role['retired_at_us']): cycle.clock(at,self.window)
            assert refreshed <= actual['released_at_us'] <= role['retired_at_us']
            assert actual['released_at_us'] < actual['lease_expires_at_us']
            state,retired_at,method='released',role['retired_at_us'],'storage_fenced'
            assert role['xmin']==actual['xmin']
        else:
            # Both selected refusals happen during the initial read-only
            # classifier, before any refresh, mutation or release transaction.
            assert refreshed==acquired and role['retired_at_us'] is None
            assert role['xmin']==actual['xmin']
            state,retired_at,method='active',None,None
        self.control=dict(old,epoch=old['epoch']+1,role_instance_id=rid,process_incarnation=inc,
            acquired_at_us=acquired,lease_expires_at_us=actual['lease_expires_at_us'],state=state,
            released_at_us=actual['released_at_us'] if released else None,exclusion_method=method,exclusion_digest=None,
            control_backend_pid=actual['control_backend_pid'],control_backend_start=actual['control_backend_start'],xmin=actual['xmin'])
        if released:self.control['exclusion_digest']=cycle.exclusion(self.control)
        self.pg['owner_epochs'].append(self.control)
        self.pg['role_instances'].append(dict(role_instance_id=rid,dataset_id=self.ds['dataset_id'],role='control',
            created_at_us=created,retired_at_us=retired_at,xmin=actual['xmin']))

    def empty(self, observed):
        legacy,_,native_head,epoch=coordinates(self.pg,self.native)
        assert epoch>=3 and not any(r['target_epoch']==epoch for r in self.pg['ingest_activations'])
        intent=only(self.pg['ingest_successions'],target_epoch=epoch)
        producer=only(self.pg['producer_epochs'],producer_epoch=epoch)
        assert producer['reserved_through_request']==0
        assert not any(r['producer_epoch']==epoch for r in self.pg['producer_requests']+self.pg['producer_outcomes'])
        predecessor=summary(self.pg,epoch-1)
        digest=transition.empty_digest(binary(intent['handoff']),binary(intent['target_manifest']),binary(self.slot),epoch)
        retry=producer['state']=='retired'
        if not retry:
            assert producer['state']=='open'
            if native_head<epoch: complete_gap(self.native,intent)
            close(self.native,self.slot,epoch)
            actual=only(observed['producer_epochs'],producer_epoch=epoch)
            at,xid=actual['retired_at_us'],actual['xmin']
            cycle.clock(at,self.window);cycle.fresh_xmin(xid,self.pg)
            assert max(intent['created_at_us'],self.control['acquired_at_us'])<=at<=self.control['released_at_us']
            assert at<self.control['lease_expires_at_us']
            receipt=only(observed['ingest_terminal_transactions'],target_epoch=epoch)
            assert re.fullmatch('[1-9][0-9]*',receipt['terminal_transaction'])
            assert int(receipt['terminal_transaction'])<2**64 and int(receipt['terminal_transaction'])%2**32==int(xid)
            producer.update(state='retired',retired_at_us=at,closure_digest='\\x'+digest,xmin=xid)
            only(self.pg['source_streams'],owner_epoch=epoch).update(state='closed',closed_at_us=at,last_complete_sequence=0,closure_digest='\\x'+digest,xmin=xid)
            owner=only(self.pg['owner_epochs'],resource_id=self.resource,epoch=epoch)
            owner.update(state='released',released_at_us=at,exclusion_method='storage_fenced',exclusion_digest='\\x'+digest,xmin=xid)
            only(self.pg['role_instances'],role_instance_id=owner['role_instance_id']).update(retired_at_us=at,xmin=xid)
            domains=[r for r in self.pg['allocation_domains'] if r['owner_epoch']==epoch];assert len(domains)==9
            for row in domains:row.update(state='closed',closed_at_us=at,closure_digest='\\x'+digest,xmin=xid)
            self.pg['ingest_terminal_transactions'].append(dict(owner_resource_id=self.resource,target_epoch=epoch,
                terminal_transaction=receipt['terminal_transaction'],control_resource=self.control['resource_id'],control_epoch=self.control['epoch'],
                control_incarnation=self.control['process_incarnation'],xmin=xid))
        else:
            assert native_head==epoch and summary(self.pg,epoch)['digest']==digest
        same(self.pg,observed)
        return dict(format='bigbrotr-ingest-recovery/1',state='successor-empty-attempt-retired',dataset_id=self.ds['dataset_id'][2:],
            owner_resource_id=self.resource[2:],producer_slot_id=self.slot[2:],producer_epoch=str(epoch),issued_through='0',reserved_through='0',
            resolved_requests='0',verified_through=str(epoch if retry else epoch-1),
            predecessor=dict(producer_epoch=str(epoch-1),resolved_requests=str(predecessor['resolved_requests']),
                last_complete_sequence=str(predecessor['last_complete_sequence']),sha256=predecessor['digest']),
            empty_attempt_retirement=dict(owner_epoch=str(epoch),incarnation=intent['target_incarnation'][2:],
                retired_at_us=str(producer['retired_at_us']),sha256=digest,allocation_domains='9',native_acknowledged='0',
                native_retired='0',owner_release='committed'),native='closed',owner_release='committed',ready=False)
