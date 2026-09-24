"""Closed SQL expectations for the browser fixture, independent of product code."""
def session_mutation(before, after, sessions, operation, actor):
    assert operation in ('issue', 'revoke') and before.keys() == after.keys()
    for table in before.keys() - {'management_audit'}:
        assert before[table] == after[table], table
    old, new = before['management_audit'], after['management_audit']
    assert len(new) == len(old) + 1 and all(row in new for row in old)
    additions = [row for row in new if row not in old]
    assert len(additions) == 1
    added = additions[0]; row = added['row']
    assert row['action'] == 'browser.session.' + operation and row['outcome'] == 'accepted'
    assert row['actor_id'] == '\\x' + actor
    assert (row['config_revision'], row['policy_revision']) == (1, 2)
    assert all(row[key] is None for key in ['reason', 'requester_database_role', 'requester_os_uid'])
    matched = [s for s in sessions if '\\x' + s['id'] == row['subject_id']]
    assert len(matched) == 1
    session = matched[0]
    assert session['actor'] == actor and '\\x' + session['dataset'] == row['dataset_id']
    assert session['xmin'] == added['xmin']
    if operation == 'issue':
        assert session['revoked'] is None
        assert row['created_at_us'] >= session['issued']
    else:
        assert session['revoked'] is not None
        assert row['created_at_us'] >= session['revoked'] >= session['issued']
    return added
