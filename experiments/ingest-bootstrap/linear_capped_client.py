"""Sequential acquisition commands through the existing authenticated client."""
from installed_sync_client import publish
from linear_capped_transport import SELECTORS


def continue_after_prepopulation(directory,call,barrier,status,endpoint,enrollment,visibility,revisions):
    # Host independently observes A's delivered SQL/input progress before this
    # client accepts B. No socket, counter or elapsed sleep releases this gate.
    barrier('capped-prepopulated',poll=lambda:status())
    prepopulated=status(known_plan=True)
    publish(directory,'capped-prepopulated-status.json',prepopulated)
    scope=call('acquisition-scopes',dict(endpoint_id=endpoint['endpoint_id'],
        access_scope_id=enrollment['access_scope_id'],visibility=visibility,selector=SELECTORS[1],**revisions),'a2'*16)
    receipt=call('jobs',dict(kind='acquisition',scope_id=scope['scope_id'],deadline_us='120000000',**revisions),'b2'*16)
    assert receipt['execution']=='unobserved' and receipt['authority']=='acceptance-only'
    return scope,receipt
