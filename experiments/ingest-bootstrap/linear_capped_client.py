"""Sequential acquisition commands through the existing authenticated client."""
from installed_sync_client import publish
from linear_capped_transport import SELECTORS


def adoption_status(observed,scope,receipt,*,job_revision=3):
    from installed_sync_client import COMMON_SELECTOR,quantum_status
    assert type(job_revision) is int and job_revision in (3,4)
    assert {key:value for key,value in observed.items() if key!='linear_quantum'}==dict(
        format='bigbrotr-job-status/1',job_id=receipt['job_id'],kind='acquisition',state='queued',job_revision=str(job_revision),
        checkpoint_revision='0',created_at_us=receipt['accepted_at_us'],completed_at_us=None,
        observed_at_us=observed['observed_at_us'],reason_code=None,authority='observation-only')
    entries=dict(enumerate((('request',0,86400,86400),('request',0,24,86400),('request',0,23,86400),
        ('request',0,2,86400),('request',0,1,86400)),1))
    value=quantum_status(observed,scope['scope_id'],receipt['accepted_at_us'],known_plan=True,
        selector=COMMON_SELECTOR,expected_entries=entries)
    assert value==dict(format='bigbrotr-linear-quantum-status/2',quantum_number='5',scope_id=scope['scope_id'],entry_kind='request',
        disposition='window_closed',lower='0',upper='1',window_end='86400',horizon='86400',window_width='86400',query_budget=128,
        queries_spent=5,attempted_prefix='86400',gap_count=2,pending_work='settled',pending_interval=None,result='linear_observed',
        terminal='eose',gap=None,continuation_upper='86400',retained_events=0,classified_prefix=0,coverage='pending')
    return value


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


def run_adoption(directory,request,cli,logged_in,session,saved,auth):
    """One real accepted job and listener through the declared lifecycle gate.

    The host releases the second gate after five settled terminals, or after
    a declared failed seal. Either negative path makes no successor call.
    NOT_RUN here: installed lifecycle qualification belongs to the host runner.
    """
    import json
    import threading
    import time
    from installed_sync_client import COMMON_SELECTOR,relay_listener
    from linear_capped_transport import relay
    fault=json.loads((directory/'linear-quantum-fault.json').read_text())
    assert fault in ('plan-before','plan-after')
    seal_path=directory/'linear-quantum-seal-fault.json'
    seal_fault=json.loads(seal_path.read_text()) if seal_path.exists() else None
    assert seal_fault is None or (fault=='plan-before' and seal_fault in ('before-forward','after-complete'))
    adoption_path=directory/'linear-quantum-adoption-fault.json'
    adoption_fault=json.loads(adoption_path.read_text()) if adoption_path.exists() else None
    assert adoption_fault is None or (fault=='plan-before' and seal_fault is None
        and adoption_fault in ('before-forward','after-complete'))
    authority_path=directory/'linear-quantum-authority-refusal.json'
    authority_refusal=json.loads(authority_path.read_text()) if authority_path.exists() else None
    assert authority_refusal is None or (fault=='plan-before' and seal_fault is adoption_fault is None
        and authority_refusal in ('actor-grant','original-expiry'))
    integrity_path=directory/'linear-quantum-integrity-refusal.json'
    integrity_refusal=json.loads(integrity_path.read_text()) if integrity_path.exists() else None
    assert integrity_refusal is None or (fault=='plan-before' and seal_fault is adoption_fault is authority_refusal is None
        and integrity_refusal=='workflow-effect')
    retry_path=directory/'linear-quantum-seal-retry.json'
    seal_retry=json.loads(retry_path.read_text()) if retry_path.exists() else None
    assert seal_retry is None or (seal_retry=='exact' and fault=='plan-before' and seal_fault is not None
        and adoption_fault is authority_refusal is integrity_refusal is None)
    handoff_path=directory/'linear-quantum-handoff.json'
    handoff=json.loads(handoff_path.read_text()) if handoff_path.exists() else None
    assert handoff is None or (handoff=='zero-query' and fault=='plan-before'
        and seal_fault is seal_retry is adoption_fault is authority_refusal is integrity_refusal is None)
    observations=[];errors=[]
    def call(path,value,key):
        code,answer=request('POST','/management/v1/'+path,value,{**auth,'Idempotency-Key':key})
        observations.append(dict(path=path,input=value,key=key,status=code,body=answer))
        assert code==202 and answer['created'] is True and answer['command_key']==key
        return answer
    def barrier(name,seconds):
        publish(directory,name+'.json',observations)
        deadline=time.monotonic()+seconds
        while not (directory/(name+'.continue')).exists():
            assert not errors and time.monotonic()<deadline,name
            time.sleep(.01)
    visibility=json.loads((directory/'visibility.json').read_text())
    assert visibility['ordinal']=='3'
    revisions=dict(expected_config_revision='1',expected_policy_revision='1')
    endpoint=call('relay-endpoints',dict(url='WS://127.0.0.1:7447/a/../installed-fixture',**revisions),'91'*16)
    enrollment=call('relay-enrollments',dict(endpoint_id=endpoint['endpoint_id'],visibility=visibility,
        route={'class':'private','cidr':'127.0.0.1/32'},enabled=True,expected_generation=None,**revisions),'92'*16)
    assert enrollment['generation']=='1' and enrollment['enabled'] is True
    scope=call('acquisition-scopes',dict(endpoint_id=endpoint['endpoint_id'],access_scope_id=enrollment['access_scope_id'],
        visibility=visibility,selector=COMMON_SELECTOR,**revisions),'a1'*16)
    listener=relay_listener();finished=threading.Event()
    worker=threading.Thread(target=relay,args=(directory,listener,finished,errors),kwargs=dict(adoption=True))
    worker.start()
    try:
        barrier('prepared',15)
        job_input=dict(kind='acquisition',scope_id=scope['scope_id'],deadline_us='120000000',**revisions)
        receipt=call('jobs',job_input,'b1'*16)
        assert receipt['execution']=='unobserved' and receipt['authority']=='acceptance-only'
        # No HTTP poll while the original process is deliberately lost. The
        # same authenticated session later observes the same accepted JobId.
        if fault=='plan-after' or seal_fault is not None or adoption_fault is not None or authority_refusal is not None or integrity_refusal is not None:
            completion=('seal-commit-retry' if seal_retry is not None else 'continuation-integrity-refusal' if integrity_refusal is not None else
                'continuation-authority-refusal' if authority_refusal is not None else
                'adoption-commit-fault' if adoption_fault is not None else
                'seal-commit-fault' if seal_fault is not None else 'unsettled-seal-refusal')
            publish(directory,'adoption-old-cut.json',observations)
            # Genuine expiry consumes the original declared duration before a
            # fresh startup. Reuse the existing20s settlement allowance only for
            # this observation gate; the job deadline and all old110s gates stay.
            seconds=int(job_input['deadline_us'])/1_000_000+20 if authority_refusal=='original-expiry' else 110
            barrier(completion,seconds)
            assert json.loads(session.read_text())==saved
            result=dict(login=logged_in,observations=observations,job_status=None,completion=completion)
            if authority_refusal is not None:result.update(authority_refusal=authority_refusal,gate_seconds=seconds)
            if integrity_refusal is not None:result.update(integrity_refusal=integrity_refusal,gate_seconds=seconds)
            if seal_retry is not None:result.update(seal_retry=seal_retry,gate_seconds=seconds)
        else:
            barrier('adoption-old-cut',110)
            code,observed=request('GET','/management/v1/jobs/'+receipt['job_id'],headers=auth)
            assert code==200
            adoption_status(observed,scope,receipt,job_revision=4 if handoff else 3)
            publish(directory,'linear-status-observations.json',[observed])
            logged_out=cli('logout','--session-ref','file:'+str(session))
            assert json.loads(session.read_text())==saved
            assert request('GET','/management/v1/me',headers=auth)[0]==401
            barrier('commands-finished',15)
            result=dict(login=logged_in,logout=logged_out,observations=observations,job_status=observed)
            if handoff is not None:result.update(handoff=handoff,gate_seconds=110)
    finally:
        finished.set();worker.join(4)
        assert not worker.is_alive(),'adoption relay did not join'
    assert not errors
    return result
