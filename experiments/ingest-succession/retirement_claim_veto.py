#!/usr/bin/env python3
"""Keep a genuine queued zero-request claim unresolved through retirement refusal.

This fresh legacy-empty copy prepares/activates3 and accepts/claims through the
public typed APIs while its activator is still alive. Fresh source closure then
retirement must refuse the unresolved claim. No abandonment, row restoration or
historical authority synthesis is part of this separate suite.
"""
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import retirement

transition = retirement.transition
STEPS = (
    ('empty-claims3', 'acquisition-claims', 'successor-lineage-acquisition-claims',
     'lineage-acquisition-complete', 'PASS'),
    ('empty-source3', 'source', 'successor-lineage-source', 'lineage-source-complete', 'PASS'),
    ('empty-retire-refusal3', 'retire', 'successor-lineage-retire',
     'lineage-retirement-complete', 'PASS_EXPECTED_REFUSAL'),
)
CASES = tuple(step[0] for step in STEPS)
SUITE = transition.Suite(
    directory='retirement-claim-veto', label='org.bigbrotr.succession-retirement-claim-veto',
    status='PASS_SUCCESSOR_RETIREMENT_CLAIM_VETO',
    client_prefix='bigbrotr-successor-retirement-claim-', database_prefix='bb_successor_retire_claim_',
    clones=('empty',), cases=CASES,
    scope='Genuine activated3 queued zero-request acquisition claim and accepted-unclaimed intent, source closure, and retirement refusal with workflow unresolved',
    not_run=('Abandonment or eventual retirement of this deliberately unresolved clone',
             'Retirement COMMIT loss, cancellation/expiry, claim bounds and import/checkpoint guard cases',
             'Installed repeated lifetimes, import closure, ACK/GC and Archive'))


def run_sequence(pg, client, output, clone, directory, expected, report):
    assert clone['name'] == clone['origin'] == 'empty'
    return retirement._sequence(pg, client, output, clone, directory, expected, report, STEPS)


def run(root, build):
    # Keep the established implicit origin identity and original preservation:
    # both old origins are checked, only empty is cloned, and no final acquired
    # state is projected backwards into a historical unresolved claim.
    transition.run(root, build, suite=SUITE, sequence=run_sequence, extra_grants=retirement.GRANTS)


if __name__ == '__main__':
    assert len(sys.argv) == 3
    run(Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2]).resolve(strict=True))
