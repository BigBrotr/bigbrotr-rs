"""Independent release-COMMIT observations; neither outcome grants recovery authority."""
import re

from cases.first_launch import require
from cases import first_launch_index_oracle as index
from cases.first_launch_index_pin import AUTHORITY, PIN_FIXED, number, reader_identity
from cases.first_launch_restart import login_buckets

MODES = ('before-forward', 'after-complete')
PIN_TABLES = ('snapshot_leases', 'archive_snapshot_pins', 'retention_claims')


def owner_fixed(before, after):
    index.exact({k:v for k,v in before.items() if k not in ('lease_expires_at_us','xmin')},
                {k:v for k,v in after.items() if k not in ('lease_expires_at_us','xmin')})
    require(number(after['lease_expires_at_us']) >= number(before['lease_expires_at_us']), 'owner lease regressed')
    require(re.fullmatch('[1-9][0-9]*',before['xmin']) and re.fullmatch('[1-9][0-9]*',after['xmin'])
            and ((before['xmin'] == after['xmin']) == (before['lease_expires_at_us'] == after['lease_expires_at_us'])),
            'only the current owner renewal may change its MVCC version')


def row_value(row):
    # The all-table owner includes xmin; row_to_json from the bounded live
    # sampler does not. Keep both raw captures and check the version separately.
    return {k:v for k,v in row.items() if k != 'xmin'}


def selected_pin(sample, owner):
    lease, pin = sample['lease'], sample['pin']
    require(lease['state'] == 'active' and number(lease['lease_revision']) == 1
            and lease['released_at_us'] is lease['release_reason'] is None, 'original active lease')
    require(0 < number(lease['expires_at_us'])-number(lease['acquired_at_us']) <= 3_000_000,
            'original owner-capped pin')
    require(lease['acquired_at_us'] <= sample['now_us'] < lease['expires_at_us'], 'live selected pin')
    require(pin['purpose'] == 'reader' and pin['lease_id'] == lease['lease_id']
            and pin['issuer_role'] == 'control' and number(pin['requested_ttl_us']) > 5_000_000,
            'actual full-probe reader purpose')
    index.exact([pin[k] for k in ('dataset_id','issuer_resource','issuer_epoch','issuer_incarnation')],
                [owner[k] for k in ('dataset_id','resource_id','epoch','process_incarnation')])
    index.exact([lease[k] for k in ('dataset_id','holder_kind','holder_job_id','holder_role_instance_id','holder_role','holder_incarnation')],
                [owner['dataset_id'],'role_instance',None,owner['role_instance_id'],'control',owner['process_incarnation']])
    require(lease['consumer_id'] == pin['consumer_id'] and lease['frontier_revision'] == pin['frontier_revision'] == 4
            and lease['topology_generation'] == 0 and lease['archive_pin_protocol'] == 1, 'current complete published cut')
    claim, = sample['claims']
    index.exact([claim[k] for k in ('dataset_id','lease_id','topology_generation','shard_id','owner_epoch','first_sequence','last_sequence','purpose','state','released_at_us','release_reason')],
                [lease['dataset_id'],lease['lease_id'],0,0,1,1,12,'reader','active',None,None])


def setup(before, after, principal, clock_before, clock_after):
    """One new ordinary owner/login and exactly its first active readiness pin."""
    require(set(before) == set(after) and len(before) == 81, 'complete setup inventory')
    changed = {'owner_epochs','role_instances','browser_sessions','login_buckets','management_audit',*PIN_TABLES}
    for table in before.keys()-changed:
        index.exact(after[table],before[table])
    for table in ('owner_epochs','role_instances','management_audit',*PIN_TABLES):
        require(all(row in after[table] for row in before[table]), 'setup changed earlier '+table)
    owner, = [row for row in after['owner_epochs'] if row not in before['owner_epochs']]
    previous = max((row for row in before['owner_epochs'] if row['role'] == 'control'),key=lambda r:r['epoch'])
    require(previous['state'] == 'released' and owner['state'] == 'active' and owner['role'] == 'control'
            and owner['resource_id'] == previous['resource_id'] and owner['epoch'] == previous['epoch']+1
            and owner['released_at_us'] is None and clock_before <= owner['acquired_at_us'] <= clock_after,
            'fresh ordinary authority after original committed release')
    role, = [row for row in after['role_instances'] if row not in before['role_instances']]
    require(role['role_instance_id'] == owner['role_instance_id'] and role['dataset_id'] == owner['dataset_id']
            and role['role'] == 'control' and role['retired_at_us'] is None, 'actual retained ordinary role')
    old, = before['browser_sessions']; session, = after['browser_sessions']
    require(old['revoked_at_us'] is not None and old['principal_id'] == session['principal_id'] == principal
            and old['session_id'] != session['session_id'] and session['revoked_at_us'] is None
            and clock_before <= session['issued_at_us'] <= clock_after
            and session['last_seen_at_us'] == session['issued_at_us'], 'fresh login with status-only idle history')
    audit, = [row for row in after['management_audit'] if row not in before['management_audit']]
    require(audit['action'] == 'browser.session.issue' and audit['outcome'] == 'accepted'
            and audit['actor_id'] == principal and audit['subject_id'] == session['session_id']
            and audit['dataset_id'] == owner['dataset_id'] and audit['config_revision'] == audit['policy_revision'] == 1,
            'independent session issuance audit')
    require(len(before['login_buckets']) == len(after['login_buckets']) == 2, 'closed login bucket inventory')
    for row in after['login_buckets']:
        previous, = [r for r in before['login_buckets'] if r['kind'] == row['kind']]
        bucket, maximum = login_buckets()[row['kind']]
        require(row['bucket'] == previous['bucket'] == bucket and row['dataset_id'] == previous['dataset_id'] == owner['dataset_id'], 'login bucket identity')
        if row['window_started_at_us'] == previous['window_started_at_us']:
            require(row['attempts'] == previous['attempts']+1 <= maximum, 'single login attempt')
        else:
            require(clock_before <= row['window_started_at_us'] <= clock_after
                    and row['window_started_at_us']-previous['window_started_at_us'] >= 60_000_000
                    and row['attempts'] == 1, 'real login window reset')
    new = {row['lease_id'] for row in after['snapshot_leases'] if row not in before['snapshot_leases']}
    require(len(new) == 1, 'exactly one first-probe reader')
    for table in PIN_TABLES:
        require(len([r for r in after[table] if r['lease_id'] in new]) == 1, 'one first-probe claim and pin')
        index.exact([r for r in after[table] if r['lease_id'] not in new],before[table])
    for key in new:
        lease, = [r for r in after['snapshot_leases'] if r['lease_id'] == key]
        pin, = [r for r in after['archive_snapshot_pins'] if r['lease_id'] == key]
        claim, = [r for r in after['retention_claims'] if r['lease_id'] == key]
        selected_pin(dict(lease=lease,pin=pin,claims=[claim],now_us=clock_after),owner)
        consumer, = [r for r in after['consumers'] if r['consumer_role'] == 'archive']
        index.exact([lease[k] for k in ('consumer_id','frontier_revision','topology_generation')],
                    [consumer[k] for k in ('consumer_id','current_revision','current_topology_generation')])
        require(clock_before <= lease['acquired_at_us'] == lease['updated_at_us']
                and lease['xmin'] == pin['xmin'] == claim['xmin'], 'original atomic first-probe admission')
    return owner


def setup_roles(value, state, clock_before, clock_after):
    index.ordinary_roles(value,state,clock_before,clock_after)
    for cap in value['capabilities']:
        require(cap['state'] == 'building' and cap['reason'] == 'verification_in_flight'
                and cap['time'] is cap['published'] is None, 'fresh first probe in flight with empty operation cache')


def admission_versions(gate, observed, before):
    """Bind the selected raw setup rows to the live gate before fault arming."""
    lease_id = gate['lease']['lease_id']
    lease, = [r for r in before['snapshot_leases'] if r['lease_id'] == lease_id]
    pin, = [r for r in before['archive_snapshot_pins'] if r['lease_id'] == lease_id]
    claim, = [r for r in before['retention_claims'] if r['lease_id'] == lease_id]
    for captured,live,raw in ((lease,gate['lease'],observed['lease']),
                              (pin,gate['pin'],observed['pin'])):
        index.exact(row_value(captured),live); index.exact(raw,live)
    index.exact([row_value(claim)],gate['claims']); index.exact(observed['claims'],gate['claims'])
    index.exact([observed['lease_xmin'],observed['pin_xmin'],*observed['claim_xmins']],
                [lease['xmin'],pin['xmin'],claim['xmin']])


def verify(evidence, before, after):
    mode = evidence['mode']; require(mode in MODES, 'closed COMMIT fault mode')
    gate, held, versions = evidence['gate'],evidence['held'],evidence['versions']
    owner = gate['owner']; selected_pin(gate,owner)
    index.exact([owner[k] for k in AUTHORITY],[evidence['baseline_owner'][k] for k in AUTHORITY])
    index.exact(gate['control_backend'],dict(pid=owner['control_backend_pid'],backend_start=owner['control_backend_start']))
    index.exact(evidence['owner_backend'],dict(pid=owner['control_backend_pid'],backend_start=owner['control_backend_start'],
        usename='bigbrotr_control',datname='bigbrotr_control'))
    require(owner['state'] == 'active' and owner['released_at_us'] is None
            and gate['now_us'] < number(owner['lease_expires_at_us']) <= gate['now_us']+3_100_000,
            'same actual live Control authority at admission')
    reader_identity(gate['reader'],gate['lease']['acquired_at_us'])
    require(evidence['arm_clock_us'] <= gate['lease']['acquired_at_us']
            and gate['reader']['backend_start_us'] <= gate['now_us']
            and gate['now_us'] <= evidence['release_gate_clock_us'] < gate['lease']['expires_at_us'], 'new actual gated reader released while live')
    index.exact(gate['parent'],evidence['parent']); index.exact(gate['worker'],evidence['worker'])
    require(evidence['worker']['ppid'] == evidence['parent']['pid'] and re.fullmatch('[0-9a-f]{64}',evidence['worker']['sha256']), 'verified actual native child')
    require(held['worker'] is None and held['custody']['busy'] is True
            and held['control_backend_pid'] == owner['control_backend_pid'], 'joined native with retained custody at exact release UPDATE1')
    index.exact(held['parent'],evidence['parent'])
    index.exact(held['custody']['identity'],evidence['custody']['identity'])
    require(evidence['custody']['busy'] is True and evidence['terminal_custody']['busy'] is False, 'custody held then positively available after join')
    index.exact(evidence['terminal_custody']['identity'],evidence['custody']['identity'])
    require(evidence['setup_started_ns'] <= evidence['started_ns'] <= gate['started_ns'] < gate['finished_ns']
            <= evidence['setup_finished_ns'] <= held['started_ns'] <= held['finished_ns'] < evidence['allow_ns']
            and evidence['allow_ns']-held['started_ns'] < 500_000_000
            and 0 < evidence['setup_finished_ns']-evidence['setup_started_ns'] < 30_000_000_000
            and evidence['finished_ns']-evidence['started_ns'] < 30_000_000_000, 'unchanged bounded operation and focused phase')
    for event in (evidence['update'],evidence['fault']):
        require(event['backend_pid'] == owner['control_backend_pid'] and event['hold_pin_release'] is True
                and event['target'] == 'control-pin-release' and event['mode'] == mode, 'selected owner backend fault')
    hold_event = evidence['hold_event']
    require(hold_event['stage'] == 'pin-release-update' and hold_event['backend_pid'] == owner['control_backend_pid']
            and hold_event['hold_pin_release'] is True, 'exact eligible completion held')
    for event in (evidence['update'],hold_event):
        index.exact([event[k] for k in ('data_queries','data_commits','server_complete','client_complete','faults')],[1,0,0,0,0])
    index.exact([evidence['fault'][k] for k in ('data_queries','data_commits','server_complete','client_complete','faults')],
                [1,1,int(mode == 'after-complete'),0,1])
    require(evidence['terminal']['exit_code'] == 4 and evidence['terminal']['parent'] is None
            and evidence['terminal']['worker'] is None and evidence['terminal']['backend_count'] == 0
            and evidence['terminal']['http'] == 'connection-refused'
            and evidence['terminal']['commit_unknown'] is True and evidence['terminal']['owner_release_claimed'] is False,
            'actual unavailable CommitUnknown terminal without owner release')
    index.ordinary_unchanged(before,after)
    lease_id = gate['lease']['lease_id']
    for table in PIN_TABLES:
        index.exact([r for r in after[table] if r['lease_id'] != lease_id],
                    [r for r in before[table] if r['lease_id'] != lease_id])
        require(len([r for r in after[table] if r['lease_id'] == lease_id])
                == len([r for r in before[table] if r['lease_id'] == lease_id]) == 1, 'one selected release obligation in both raw captures')
    admission_versions(gate,versions['before'],before)
    baseline_owner = evidence['baseline_owner']
    key = lambda r:(r['resource_id'],r['epoch'])
    index.exact([r for r in after['owner_epochs'] if key(r) != key(owner)],
                [r for r in before['owner_epochs'] if key(r) != key(owner)])
    final_owner, = [r for r in after['owner_epochs'] if key(r) == key(owner)]
    owner_fixed(baseline_owner,final_owner)
    require(final_owner['state'] == 'active' and final_owner['released_at_us'] is None, 'unknown release must retain owner history')
    final, = [r for r in after['snapshot_leases'] if r['lease_id'] == lease_id]
    pin, = [r for r in after['archive_snapshot_pins'] if r['lease_id'] == lease_id]
    claim, = [r for r in after['retention_claims'] if r['lease_id'] == lease_id]
    index.exact(row_value(pin),gate['pin'])
    index.exact([final[k] for k in PIN_FIXED],[gate['lease'][k] for k in PIN_FIXED])
    index.exact(versions['before']['lease'],gate['lease'])
    index.exact(versions['before']['claims'],gate['claims'])
    index.exact(versions['after']['lease'],row_value(final)); index.exact(versions['after']['claims'],[row_value(claim)])
    for observed in versions.values():
        index.exact(observed['pin'],gate['pin'])
        require(observed['pin_xmin'] == pin['xmin'], 'immutable pin binding MVCC version')
    require(versions['after']['lease_xmin'] == final['xmin']
            and versions['after']['claim_xmins'] == [claim['xmin']], 'raw capture and bounded observer versions agree')
    for observed in versions.values():
        require(re.fullmatch('[1-9][0-9]*',observed['lease_xmin']) and len(observed['claim_xmins']) == 1
                and re.fullmatch('[1-9][0-9]*',observed['claim_xmins'][0]), 'raw MVCC transaction identity')
    if mode == 'before-forward':
        index.exact(row_value(final),gate['lease']); index.exact([row_value(claim)],gate['claims'])
        index.exact(versions['after'],versions['before'])
    else:
        require(final['state'] == claim['state'] == 'released' and number(final['lease_revision']) == 2
                and final['released_at_us'] == final['updated_at_us']
                and final['release_reason'] == claim['release_reason'] == 'reader_joined'
                and gate['now_us'] <= claim['released_at_us'] <= final['released_at_us'], 'actual atomic joined release')
        changed = {'state','lease_revision','released_at_us','updated_at_us','release_reason','xmin'}
        index.exact({k:v for k,v in final.items() if k not in changed},{k:v for k,v in gate['lease'].items() if k not in changed})
        changed = {'state','released_at_us','release_reason','xmin'}
        index.exact({k:v for k,v in claim.items() if k not in changed},{k:v for k,v in gate['claims'][0].items() if k not in changed})
        require(versions['after']['lease_xmin'] == versions['after']['claim_xmins'][0]
                and versions['after']['lease_xmin'] != versions['before']['lease_xmin']
                and versions['after']['claim_xmins'] != versions['before']['claim_xmins'], 'same real release transaction committed')
    return dict(status='PASS',mode=mode,product='CommitUnknown',available=False,exit_code=4,
        independently_observed_pin=final['state'],owner_release='not claimed',native='positively joined',
        unknown_native_termination='NOT_RUN',recovery='not attempted')
