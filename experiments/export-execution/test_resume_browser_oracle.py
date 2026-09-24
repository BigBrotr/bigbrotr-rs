import copy
import unittest
from resume_browser_oracle import session_mutation


class SessionAuditTests(unittest.TestCase):
    def setUp(self):
        self.before = {'jobs': [{'row': {'state': 'paused'}, 'xmin': '9'}], 'management_commands': [], 'management_audit': [{'row': {'action': 'dataset.initialize'}, 'xmin': '1'}]}
        self.session = dict(id='32'*16, actor='10'*16, dataset='01'*16, issued=100, revoked=None, xmin='20')
        self.audit = dict(row=dict(audit_id='\\x'+'41'*16, dataset_id='\\x'+'01'*16, actor_id='\\x'+'10'*16, subject_id='\\x'+'32'*16, action='browser.session.issue', outcome='accepted', config_revision=1, policy_revision=2, created_at_us=105, reason=None, requester_database_role=None, requester_os_uid=None), xmin='20')
        self.after = copy.deepcopy(self.before)
        self.after['management_audit'].append(self.audit)

    def verify(self):
        return session_mutation(self.before, self.after, [self.session], 'issue', '10'*16)

    def test_issue_is_one_atomic_attributed_audit(self):
        self.assertEqual(self.verify(), self.audit)

    def test_revoke_requires_actual_revoked_session(self):
        self.audit['row']['action'] = 'browser.session.revoke'
        self.session['revoked'] = 103
        self.assertEqual(session_mutation(self.before, self.after, [self.session], 'revoke', '10'*16), self.audit)
        self.session['revoked'] = None
        with self.assertRaises(AssertionError):
            session_mutation(self.before, self.after, [self.session], 'revoke', '10'*16)

    def test_refuses_changed_job_or_old_audit_extra_row_and_bad_attribution(self):
        changes = [lambda: self.after['jobs'][0]['row'].update(state='queued'),
                   lambda: self.after['management_audit'][0].update(xmin='2'),
                   lambda: self.after['management_audit'].append(copy.deepcopy(self.audit)),
                   lambda: self.audit['row'].update(actor_id='\\x'+'99'*16),
                   lambda: self.audit['row'].update(subject_id='\\x'+'99'*16),
                   lambda: self.audit['row'].update(policy_revision=1),
                   lambda: self.audit['row'].update(created_at_us=99),
                   lambda: self.audit.update(xmin='21')]
        for change in changes:
            with self.subTest(change=changes.index(change)):
                self.setUp(); change()
                with self.assertRaises(AssertionError): self.verify()


if __name__ == '__main__': unittest.main()
