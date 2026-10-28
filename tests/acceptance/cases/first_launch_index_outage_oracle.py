"""Stopped Index backend: retained prefix, one pending plan and exact source."""
from cases import first_launch_index_oracle as index
from cases import first_launch_index_restart_oracle as restart
from cases import first_launch_archive_outage_oracle as outage


def live(prefix, state, root):
    restart.backlog(prefix,root,prefix=True)
    restart.backlog(state,root)
    outage.prefix_preservation(prefix,state,index_unavailable=True)


def native_rows(dump, wanted):
    assert set(dump)=={'event','rows','sequence'} and dump['event']=='dump-state'
    # Existing four-request native observer bound; sequence is not an ACK oracle.
    assert type(dump['sequence']) is int and 49 <= dump['sequence'] < 128
    actual=[(r['cf'],bytes.fromhex(r['key']),bytes.fromhex(r['value'])) for r in dump['rows']]
    assert len(actual)==len(set((cf,k) for cf,k,_ in actual))==49
    index.exact([(cf,k.hex(),v.hex()) for cf,k,v in actual],
                [(cf,k.hex(),v.hex()) for cf,k,v in wanted['native']])


def publication(state, actual, science, schema, root):
    restart.backlog(state,root)
    generation=index.validate_metadata(state,root)
    index.table_identity(schema,root)
    rows=index.physical_rows(actual,science,generation['generation_id'],schema,through=3)
    index.plan_science(state,science,rows,generation)
    return dict(format='bigbrotr-installed-index-backend-outage/1',source_cut=12,
        archive_revision=4,archive_cut=12,index_revision=2,index_cut=3,index_lag_records=9,
        prepared_plan=3,prepared_range=[4,7],prepared_chunks=10,unplanned_range=[8,12],
        physical_rows=3,physical_scope='independently_ready_prefix_before_backend_stop',
        native_rows=49,original_requests=4,original_inputs=4,acknowledged_through=4,retired_through=0,
        prefix_acknowledged_data='three_dictionaries',signed_event_admitted='during_outage',
        index_readiness='unavailable/dependency_unavailable',backend='stopped_retained',
        control_tables=82,automatic_recovery='NOT_RUN',remote_uncertainty_resolution='NOT_RUN',
        general_capacity='NOT_RUN',complete_m1='NOT_RUN')
