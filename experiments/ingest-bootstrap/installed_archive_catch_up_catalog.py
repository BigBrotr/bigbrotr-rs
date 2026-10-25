"""Append-only native catalog envelope; science is checked by the C API census.

Physical Parquet encodings/statistics are engine observations, not literal golden
bytes. Their immutable ancestry, schemas, snapshot/transaction associations,
file sizes and row counts are independently checked here.
"""
import re
import json
import sys
from pathlib import Path, PurePosixPath
from installed_archive_catch_up_model import only,clock
from installed_family_replay import instant


def verify_file_counts(row,file):
    # Pinned DuckLake d8a1881 stores non-null values in value_count. This
    # fixture requires complete per-record counts: unknown is refused.
    assert all(type(row[k]) is int and row[k]>=0 for k in ('value_count','null_count','column_size_bytes'))
    assert all(type(file[k]) is int and file[k]>=0 for k in ('record_count','file_size_bytes'))
    assert row['value_count']+row['null_count']==file['record_count']
    assert row['column_size_bytes']<=file['file_size_bytes']


def virgin_layout(catalog):
    """Closed DDL, not observed mappings, defines every leaf and name mapping."""
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'archive-engine'))
    from scientific_layout_oracle import expected,observed
    declaration=expected()
    observed({k.removeprefix('public.'):v for k,v in catalog.items()},declaration)
    tables={r['table_name']:r for r in catalog['public.ducklake_table']}
    assert set(tables)==set(declaration['tables'])
    leaves={};wanted=[];mapping_ids=set()
    assert len(catalog['public.ducklake_column_mapping'])==len(tables)==6
    for name,columns in declaration['tables'].items():
        table=tables[name];mapping=only(catalog['public.ducklake_column_mapping'],table_id=table['table_id'])
        mid=mapping['mapping_id'];assert type(mid) is int and mid>=0 and mid not in mapping_ids
        mapping_ids.add(mid)
        first=min(r['begin_snapshot'] for r in catalog['public.ducklake_data_file'] if r['table_id']==table['table_id'])
        tx=only(catalog['public.ducklake_snapshot'],snapshot_id=first)['xmin']
        assert mapping==dict(mapping_id=mid,table_id=table['table_id'],type='map_by_name',xmin=tx)
        byid={r[0]:r for r in columns}
        for field,column,kind,parent,_ in columns:
            # d8a1881 MapColumn renames every LIST child to list;
            # ConvertNameMapColumn numbers pre-order from0 (field IDs start1).
            source_name='list' if parent is not None and byid[parent][2]=='list' else column
            wanted.append(dict(mapping_id=mid,column_id=field-1,source_name=source_name,target_field_id=field,
                parent_column=None if parent is None else parent-1,is_partition=False,xmin=tx))
            if kind not in ('list','struct'):
                chain=[];current=field
                while current is not None:
                    row=byid[current];chain.append((row[1],row[2]));current=row[3]
                leaves[table['table_id'],field]=list(reversed(chain))
    canonical=lambda rows:sorted(rows,key=lambda r:json.dumps(r,sort_keys=True))
    assert canonical(catalog['public.ducklake_name_mapping'])==canonical(wanted)
    assert len(wanted)==203 and len(leaves)==172
    return leaves


def leaf_values(record,chain):
    """Parquet definition levels: a null/empty ancestor contributes one null."""
    values=[record];parent=None
    for name,kind in chain:
        result=[]
        for value in values:
            if value is None:result.append(None)
            elif parent=='list':result.extend(value or [None])
            else:result.append(value[name])
        values,parent=result,kind
    return values


def verify_catalog(phase,science,*,virgin=False):
    assert type(virgin) is bool
    before,after=phase['before'],phase['after'];a,b=before['catalog'],after['catalog']
    assert set(a)==set(b) and len(a)==32
    mutable={'bigbrotr_archive.attempts','bigbrotr_archive.owner',*[f'public.ducklake_{s}' for s in
        ('snapshot','snapshot_changes','data_file','file_column_stats','table_stats','table_column_stats')]}
    if virgin:mutable.update(('public.ducklake_column_mapping','public.ducklake_name_mapping'))
    for table in a.keys()-mutable:assert a[table]==b[table],table
    end=8 if virgin else 7;start=0 if virgin else 4
    assert len(b['bigbrotr_archive.attempts'])==end
    if virgin:
        for table in ('bigbrotr_archive.attempts',*[f'public.ducklake_{s}' for s in
                ('data_file','file_column_stats','table_stats','table_column_stats','column_mapping','name_mapping')]):
            assert not a[table]
    for row in a['bigbrotr_archive.attempts']:assert row in b['bigbrotr_archive.attempts']
    for ordinal in range(start+1,end+1):
        row=only(b['bigbrotr_archive.attempts'],ordinal=ordinal)
        issued=only(after['control']['archive_attempts'],ordinal=ordinal)
        assert row==dict({k:issued[k] for k in ('ordinal','attempt_id','coordinator_incarnation','worker_incarnation','topology_generation','batch_digest')},
            login_name='bb_archive_'+issued['attempt_id'][2:],login_oid=row['login_oid'],xmin=row['xmin'])
        assert str(row['login_oid']).isdecimal() and int(row['login_oid'])>0 and str(row['xmin']).isdecimal()
    owner=only(b['bigbrotr_archive.owner'])
    assert owner==dict(only(a['bigbrotr_archive.owner']),ordinal=end,xmin=owner['xmin']) and owner['enabled'] is False
    for table in ('snapshot','snapshot_changes','data_file','file_column_stats'):
        for row in a['public.ducklake_'+table]:assert row in b['public.ducklake_'+table],table
    assert sorted(r['snapshot_id'] for r in b['public.ducklake_snapshot'])==list(range(end+2))
    assert sorted(r['snapshot_id'] for r in b['public.ducklake_snapshot_changes'])==list(range(end+2))
    tables={r['table_name']:r for r in b['public.ducklake_table']}
    expected={6:{'dictionaries':4,'archive_commits':1},7:{'dictionaries':1,'events':1,'event_eligibility_facts':1,
        'event_receive_facts':1,'provenance_facts':1,'archive_commits':1},8:{'provenance_facts':1,'archive_commits':1}}
    scientific_files={}
    if virgin:
        assert len(science['batches'])==8
        expected={}
        for snapshot,batch in enumerate(science['batches'],2):
            counts={'archive_commits':1}
            for name,rows in science['tables'].items():
                selected=[r for r in rows if r['source_record_id']['owner_epoch']==batch['epoch']
                    and batch['first']<=r['source_record_id']['sequence']<=batch['last']]
                if selected:counts[name]=len(selected);scientific_files[snapshot,name]=selected
            expected[snapshot]=counts
        leaves=virgin_layout(b)
    # The immutable native layout names, rather than fixed generated table IDs,
    # associate each physical file with its independently expected family count.
    expected_paths=set(before['physical']['data']['files']);newids=set()
    last=only(a['public.ducklake_snapshot'],snapshot_id=start+1)
    for snapshot,counts in expected.items():
        row=only(b['public.ducklake_snapshot'],snapshot_id=snapshot);clock(instant(row['snapshot_time']),phase['window'])
        assert row==dict(last,snapshot_id=snapshot,snapshot_time=row['snapshot_time'],next_file_id=row['next_file_id'],xmin=row['xmin'])
        assert row['next_file_id']>last['next_file_id'] and row['xmin']!=last['xmin']
        if virgin:
            maps=[r for r in b['public.ducklake_column_mapping'] if r['xmin']==row['xmin']]
            assert {r['mapping_id'] for r in maps}==set(range(last['next_file_id'],last['next_file_id']+len(maps)))
            assert row['next_file_id']==last['next_file_id']+len(maps)+len(counts)
            assert int(row['xmin'])>int(last['xmin'])
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
            if virgin:
                assert fid>=last['next_file_id']+len(maps)
                assert record['mapping_id']==only(b['public.ducklake_column_mapping'],table_id=table['table_id'])['mapping_id']
            schema=only(b['public.ducklake_schema'],schema_id=table['schema_id'])
            path=str(PurePosixPath(schema['path'])/table['path']/record['path']);expected_paths.update((path,str(PurePosixPath(path).parent)))
            metadata=after['physical']['data']['files'][path]
            assert metadata['mode']==0o600 and metadata['links']==1 and re.fullmatch('[0-9a-f]{64}',metadata['sha256'])
        last=row
    added=sum(map(len,expected.values()))
    assert len(b['public.ducklake_data_file'])==len(a['public.ducklake_data_file'])+added and len(newids)==added
    if virgin:assert added==32
    assert set(after['physical']['data']['files'])==expected_paths
    for name,old in before['physical']['data']['files'].items():
        current=after['physical']['data']['files'][name]
        assert {k:v for k,v in old.items() if k!='links'}=={k:v for k,v in current.items() if k!='links'}
        assert current['links']==old['links'] if old['sha256'] is not None else current['links']>=old['links']
    counts={t:len(rows) for t,rows in science['tables'].items()}|{'archive_commits':end}
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
        if virgin:
            assert (row['table_id'],row['column_id']) in leaves
            assert all(type(row[k]) is int and row[k]>=0 for k in ('value_count','null_count','column_size_bytes'))
            assert row['column_size_bytes']<=file['file_size_bytes']
            name=only(b['public.ducklake_table'],table_id=row['table_id'])['table_name']
            if name=='archive_commits':
                # One non-null receipt per batch; input_ranges contains exactly
                # one bound. There are no nullable leaves in this closed table.
                assert (row['value_count'],row['null_count'])==(1,0)
            else:
                values=[v for record in scientific_files[file['begin_snapshot'],name]
                    for v in leaf_values(record,leaves[row['table_id'],row['column_id']])]
                assert row['null_count']==values.count(None) and row['value_count']==len(values)-values.count(None)
            assert (row['min_value'] is None)==(row['max_value'] is None)==(row['value_count']==0)
        else:verify_file_counts(row,file)
        assert row['contains_nan'] is None and row['extra_stats'] is None
    keys=lambda rows,key:{tuple(r[k] for k in key) for r in rows}
    assert len(keys(b['public.ducklake_file_column_stats'],('data_file_id','column_id')))==len(b['public.ducklake_file_column_stats'])
    if virgin:
        assert keys(b['public.ducklake_table_column_stats'],('table_id','column_id'))==set(leaves)
        assert len(b['public.ducklake_table_column_stats'])==len(leaves)
        assert keys(b['public.ducklake_file_column_stats'],('data_file_id','column_id'))=={
            (file['data_file_id'],column) for file in b['public.ducklake_data_file']
            for table,column in leaves if table==file['table_id']}
    else:assert keys(a['public.ducklake_table_column_stats'],('table_id','column_id'))==keys(b['public.ducklake_table_column_stats'],('table_id','column_id'))
    for row in b['public.ducklake_table_column_stats']:
        if virgin:
            assert set(row)=={'table_id','column_id','contains_null','contains_nan','min_value','max_value','extra_stats','xmin'}
            assert type(row['contains_null']) is bool and row['contains_nan'] is None and row['extra_stats'] is None
        else:
            old=only(a['public.ducklake_table_column_stats'],table_id=row['table_id'],column_id=row['column_id'])
            assert set(row)==set(old) and row['contains_nan']==old['contains_nan'] and row['extra_stats']==old['extra_stats']
            assert row['contains_null'] in (old['contains_null'],True)
        assert row['xmin']==only(b['public.ducklake_table_stats'],table_id=row['table_id'])['xmin']

    # Pinned DuckLake MergeStats compares BLOB statistics as their escaped
    # strings, not byte values. Match that metadata reduction here; it is not
    # a scientific BLOB range. Actual values are independently read in full.
    def scalar(value,kind):
        if kind.startswith(('uint','int')):return int(value)
        return value
    for row in b['public.ducklake_table_column_stats']:
        records=[r for r in b['public.ducklake_file_column_stats'] if r['table_id']==row['table_id'] and r['column_id']==row['column_id']]
        kind=columns[row['table_id'],row['column_id']]['column_type']
        for field,reduce in [('min_value',min),('max_value',max)]:
            values=[r[field] for r in records if r[field] is not None]
            assert row[field]==(reduce(values,key=lambda v:scalar(v,kind)) if values else None)
        assert row['contains_null']==any(r['null_count']>0 for r in records)
