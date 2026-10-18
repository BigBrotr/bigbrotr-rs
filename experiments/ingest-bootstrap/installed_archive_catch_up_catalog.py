"""Append-only native catalog envelope; science is checked by the C API census.

Physical Parquet encodings/statistics are engine observations, not literal golden
bytes. Their immutable ancestry, schemas, snapshot/transaction associations,
file sizes and row counts are independently checked here.
"""
import re
from pathlib import PurePosixPath
from installed_archive_catch_up_model import only,clock
from installed_family_replay import instant


def verify_catalog(phase,science):
    before,after=phase['before'],phase['after'];a,b=before['catalog'],after['catalog']
    assert set(a)==set(b) and len(a)==32
    mutable={'bigbrotr_archive.attempts','bigbrotr_archive.owner',*[f'public.ducklake_{s}' for s in
        ('snapshot','snapshot_changes','data_file','file_column_stats','table_stats','table_column_stats')]}
    for table in a.keys()-mutable:assert a[table]==b[table],table
    assert len(b['bigbrotr_archive.attempts'])==7
    for row in a['bigbrotr_archive.attempts']:assert row in b['bigbrotr_archive.attempts']
    for ordinal in (5,6,7):
        row=only(b['bigbrotr_archive.attempts'],ordinal=ordinal)
        issued=only(after['control']['archive_attempts'],ordinal=ordinal)
        assert row==dict({k:issued[k] for k in ('ordinal','attempt_id','coordinator_incarnation','worker_incarnation','topology_generation','batch_digest')},
            login_name='bb_archive_'+issued['attempt_id'][2:],login_oid=row['login_oid'],xmin=row['xmin'])
        assert str(row['login_oid']).isdecimal() and int(row['login_oid'])>0 and str(row['xmin']).isdecimal()
    owner=only(b['bigbrotr_archive.owner'])
    assert owner==dict(only(a['bigbrotr_archive.owner']),ordinal=7,xmin=owner['xmin']) and owner['enabled'] is False
    for table in ('snapshot','snapshot_changes','data_file','file_column_stats'):
        for row in a['public.ducklake_'+table]:assert row in b['public.ducklake_'+table],table
    assert sorted(r['snapshot_id'] for r in b['public.ducklake_snapshot'])==list(range(9))
    assert sorted(r['snapshot_id'] for r in b['public.ducklake_snapshot_changes'])==list(range(9))
    tables={r['table_name']:r for r in b['public.ducklake_table']}
    expected={6:{'dictionaries':4,'archive_commits':1},7:{'dictionaries':1,'events':1,'event_eligibility_facts':1,
        'event_receive_facts':1,'provenance_facts':1,'archive_commits':1},8:{'provenance_facts':1,'archive_commits':1}}
    # The immutable native layout names, rather than fixed generated table IDs,
    # associate each physical file with its independently expected family count.
    expected_paths=set(before['physical']['data']['files']);newids=set()
    last=only(a['public.ducklake_snapshot'],snapshot_id=5)
    for snapshot,counts in expected.items():
        row=only(b['public.ducklake_snapshot'],snapshot_id=snapshot);clock(instant(row['snapshot_time']),phase['window'])
        assert row==dict(last,snapshot_id=snapshot,snapshot_time=row['snapshot_time'],next_file_id=row['next_file_id'],xmin=row['xmin'])
        assert row['next_file_id']>last['next_file_id'] and row['xmin']!=last['xmin']
        records=[r for r in b['public.ducklake_data_file'] if r['begin_snapshot']==snapshot]
        assert len(records)==len(counts) and {r['table_id'] for r in records}=={tables[t]['table_id'] for t in counts}
        change=only(b['public.ducklake_snapshot_changes'],snapshot_id=snapshot)
        assert change==dict(snapshot_id=snapshot,changes_made=','.join('inserted_into_table:'+str(tables[t]['table_id'])
            for t in sorted(counts,key=lambda t:tables[t]['table_id'])),author=None,commit_message=None,commit_extra_info=None,xmin=row['xmin'])
        for name,count in counts.items():
            table=tables[name];record=only(records,table_id=table['table_id']);fid=record['data_file_id'];newids.add(fid)
            assert last['next_file_id']<=fid<row['next_file_id'] and record['xmin']==row['xmin']
            assert re.fullmatch(r'prepared-[0-9a-f]{32}/data\.parquet',record['path'])
            assert record['record_count']==count and record['row_id_start']==sum(r['record_count'] for r in b['public.ducklake_data_file']
                if r['table_id']==table['table_id'] and r['begin_snapshot']<snapshot)
            expected_record=dict(data_file_id=fid,table_id=table['table_id'],begin_snapshot=snapshot,end_snapshot=None,file_order=None,
                path=record['path'],path_is_relative=True,file_format='parquet',record_count=count,file_size_bytes=record['file_size_bytes'],
                footer_size=record['footer_size'],row_id_start=record['row_id_start'],partition_id=None,encryption_key=None,
                mapping_id=record['mapping_id'],partial_max=None,xmin=row['xmin'])
            assert record==expected_record and 0<record['footer_size']<record['file_size_bytes']<=16*1024**2
            assert any(m['mapping_id']==record['mapping_id'] and m['table_id']==table['table_id'] for m in b['public.ducklake_column_mapping'])
            schema=only(b['public.ducklake_schema'],schema_id=table['schema_id'])
            path=str(PurePosixPath(schema['path'])/table['path']/record['path']);expected_paths.update((path,str(PurePosixPath(path).parent)))
            metadata=after['physical']['data']['files'][path]
            assert metadata['mode']==0o600 and metadata['links']==1 and re.fullmatch('[0-9a-f]{64}',metadata['sha256'])
        last=row
    assert len(b['public.ducklake_data_file'])==len(a['public.ducklake_data_file'])+10 and len(newids)==10
    assert set(after['physical']['data']['files'])==expected_paths
    for name,old in before['physical']['data']['files'].items():
        current=after['physical']['data']['files'][name]
        assert {k:v for k,v in old.items() if k!='links'}=={k:v for k,v in current.items() if k!='links'}
        assert current['links']==old['links'] if old['sha256'] is not None else current['links']>=old['links']
    counts={t:len(rows) for t,rows in science['tables'].items()}|{'archive_commits':7}
    assert len(b['public.ducklake_table_stats'])==6
    for name,count in counts.items():
        tid=tables[name]['table_id'];row=only(b['public.ducklake_table_stats'],table_id=tid)
        data=[r for r in b['public.ducklake_data_file'] if r['table_id']==tid]
        latest=max(data,key=lambda r:r['begin_snapshot'])
        assert row==dict(table_id=tid,record_count=count,next_row_id=count,file_size_bytes=sum(r['file_size_bytes'] for r in data),xmin=latest['xmin'])
    columns={(r['table_id'],r['column_id']):r for r in b['public.ducklake_column']}
    for row in b['public.ducklake_file_column_stats']:
        if row['data_file_id'] not in newids:continue
        file=only(b['public.ducklake_data_file'],data_file_id=row['data_file_id'])
        assert (row['table_id'],row['column_id']) in columns and row['table_id']==file['table_id'] and row['xmin']==file['xmin']
        assert set(row)=={'data_file_id','table_id','column_id','column_size_bytes','value_count','null_count','min_value','max_value','contains_nan','extra_stats','xmin'}
        assert 0<=row['null_count']<=row['value_count'] and 0<=row['column_size_bytes']<=file['file_size_bytes']
        assert row['contains_nan'] is None and row['extra_stats'] is None
    keys=lambda rows,key:{tuple(r[k] for k in key) for r in rows}
    assert len(keys(b['public.ducklake_file_column_stats'],('data_file_id','column_id')))==len(b['public.ducklake_file_column_stats'])
    assert keys(a['public.ducklake_table_column_stats'],('table_id','column_id'))==keys(b['public.ducklake_table_column_stats'],('table_id','column_id'))
    for row in b['public.ducklake_table_column_stats']:
        old=only(a['public.ducklake_table_column_stats'],table_id=row['table_id'],column_id=row['column_id'])
        assert set(row)==set(old) and row['contains_nan']==old['contains_nan'] and row['extra_stats']==old['extra_stats']
        assert row['contains_null'] in (old['contains_null'],True)
        assert row['xmin']==only(b['public.ducklake_table_stats'],table_id=row['table_id'])['xmin']

    # Native scalar extrema are checked as exact reductions of all retained
    # file statistics. Newly encoded file statistics remain observed metadata;
    # actual scientific values are independently read and compared in full.
    def scalar(value,kind):
        if kind.startswith(('uint','int')):return int(value)
        if kind=='blob':
            result=bytearray();offset=0
            while offset<len(value):
                if value.startswith('\\x',offset):result.append(int(value[offset+2:offset+4],16));offset+=4
                else:result.extend(value[offset].encode());offset+=1
            return bytes(result)
        return value
    for row in b['public.ducklake_table_column_stats']:
        records=[r for r in b['public.ducklake_file_column_stats'] if r['table_id']==row['table_id'] and r['column_id']==row['column_id']]
        kind=columns[row['table_id'],row['column_id']]['column_type']
        for field,reduce in [('min_value',min),('max_value',max)]:
            values=[r[field] for r in records if r[field] is not None]
            assert row[field]==(reduce(values,key=lambda v:scalar(v,kind)) if values else None)
        assert row['contains_null']==any(r['null_count']>0 for r in records)
