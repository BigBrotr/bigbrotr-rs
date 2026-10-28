"""Bounded writer query lifecycle observations; no query termination proof."""
import json
from pathlib import Path
import re
import shutil
import xml.etree.ElementTree as ET

from cases.first_launch import digest, require, save

TRACE_ROWS=8192
TRACE_BYTES=4*1024**2
TRACE_COLUMNS={'event','query_id','initial_query_id','user','initial_user','event_us','start_us',
               'duration_ms','exception_code','is_initial_query'}
TRACE_SETTINGS={'log_queries':'1','log_queries_min_type':"'QUERY_START'",'log_queries_min_query_duration_ms':'0',
                'log_queries_probability':'1','log_queries_cut_to_length':'1','log_query_threads':'0',
                'log_query_settings':'0','log_profile_events':'0'}


def query_trace_profile(server,users,provision):
    """Opt in only this closed writer; credential-bearing provisioner stays off."""
    require(ET.fromstring(users).findtext('./profiles/default/log_queries')=='0','default query logging must stay disabled')
    root=ET.fromstring(server)
    require(root.tag=='clickhouse' and root.find('query_log') is None,'fresh query lifecycle configuration')
    log=ET.SubElement(root,'query_log')
    for name,value in {'database':'system','table':'query_log','flush_interval_milliseconds':'7500',
                       'max_size_rows':'1024','reserved_size_rows':'64','buffer_size_rows_flush_threshold':'512',
                       'flush_on_crash':'false'}.items():ET.SubElement(log,name).text=value
    lines=provision.splitlines(keepends=True)
    selected=[n for n,line in enumerate(lines) if line.startswith('CREATE USER writer ')]
    require(len(selected)==1,'one stable Index writer')
    n=selected[0];marker=' SETTINGS max_concurrent_queries_for_user=1 MIN 1 MAX 1;'
    require(lines[n].count(marker)==1,'unchanged writer concurrency constraint')
    lines[n]=lines[n].replace(marker,marker[:-1]+', '+', '.join(k+'='+v for k,v in TRACE_SETTINGS.items())+';')
    return ET.tostring(root,encoding='unicode')+'\n',''.join(lines)


def lifecycle_summary(count,rows,positive):
    """Logged lifecycle pairs are observations, never a ProcessList removal proof."""
    require(type(count) is int and 0<=count<=TRACE_ROWS and len(rows)==count,'query lifecycle count/capture incomplete')
    groups={}
    for row in rows:
        require(set(row)==TRACE_COLUMNS and row['user']==row['initial_user']=='writer'
            and row['is_initial_query']==1,'query lifecycle owner/columns')
        query=row['query_id']
        require(query==positive or re.fullmatch(r'bigbrotr-index-[0-9a-f]{32}-[1-9][0-9]*-[1-9][0-9]*',query),
                'unexpected Index query identity')
        require(row['initial_query_id']==query and row['event'] in
            ('QueryStart','QueryFinish','ExceptionBeforeStart','ExceptionWhileProcessing'),'query lifecycle identity/event')
        require(all(type(row[k]) is int and row[k]>=0 for k in ('event_us','start_us','duration_ms','exception_code'))
            and row['event_us']>=row['start_us'],'query lifecycle time/code')
        groups.setdefault(query,[]).append(row)
    control=groups.get(positive,[])
    require(len(control)==2 and {r['event'] for r in control}=={'QueryStart','QueryFinish'}
        and all(r['exception_code']==0 for r in control),'actual named writer query lifecycle missing')
    missing=[]
    for query,events in groups.items():
        kinds=[r['event'] for r in events]
        paired=(len(kinds)==2 and kinds.count('QueryStart')==1 and
                sum(kinds.count(k) for k in ('QueryFinish','ExceptionWhileProcessing'))==1)
        before=kinds==['ExceptionBeforeStart']
        if not (paired or before):missing.append(query)
    return dict(status='INCOMPLETE' if missing else 'CAPTURED',rows=count,queries=len(groups),
        missing_or_duplicate_lifecycle=sorted(missing),exceptions=[dict(query_id=r['query_id'],event=r['event'],
            event_us=r['event_us'],exception_code=r['exception_code']) for r in rows if r['exception_code']],
        all_query_coverage='NOT_PROVEN',termination_or_exclusion='NOT_PROVEN',query_finish_is_process_list_removal=False)


def query_trace_selection(start_through_us=None):
    selected="user='writer' AND startsWith(query_id,'bigbrotr-index-')"
    if start_through_us is not None:
        require(type(start_through_us) is int and 0<start_through_us<=2**63-1,
                'query lifecycle prefix cutoff must be positive signed64 microseconds')
        selected+=' AND toUnixTimestamp64Micro(query_start_time_microseconds)<='+str(start_through_us)
    return selected


class QueryTraceRuntime:
    def candidate(self):
        path=Path(__file__)
        require(digest(path)==digest(self.source/'tests/acceptance/cases'/path.name),
                'Index query trace acceptance source mismatch')
        super().candidate()

    def prepare_state(self):
        super().prepare_state()
        # Declare the existing schema default before initialization, so the
        # observer can bind its cap to the actual deployed root without reading
        # a restart-only setting from the managed policy document.
        self.config.write_text(self.set_value(self.config.read_text(),'runtime','renewal_timeout','5s'))
        self.inputs['initial_recipe_sha256']=digest(self.config)
        private=self.state/'clickhouse/private'
        server,provision=query_trace_profile((private/'server.xml').read_text(),(private/'users.xml').read_text(),
                                            (private/'provision.sql').read_text())
        (private/'server.xml').write_text(server);(private/'provision.sql').write_text(provision)
        self.trace_output=self.root/'clickhouse-query-lifecycle';self.trace_output.mkdir(mode=0o700)
        self.trace_positive='bigbrotr-index-trace-'+self.owner
        save(self.trace_output/'profile.json',dict(writer_settings=TRACE_SETTINGS,queue_rows=1024,reserved_rows=64,
            flush_threshold_rows=512,flush_milliseconds=7500,capture_rows=TRACE_ROWS,capture_bytes=TRACE_BYTES,
            capture_seconds=30,query_text_limit_bytes=1,background_overhead=True,engine_limits='unchanged'))

    def bootstrap(self):
        super().bootstrap()
        observed=self.ch("SELECT name,value,min,max FROM system.settings WHERE name IN ('max_concurrent_queries_for_user',"
            "'log_queries','log_queries_cut_to_length','log_query_threads','log_query_settings','log_profile_events') ORDER BY name",
            'writer',rows=True)
        by_name={row['name']:row for row in observed}
        require(set(by_name)=={'max_concurrent_queries_for_user','log_queries','log_queries_cut_to_length',
            'log_query_threads','log_query_settings','log_profile_events'},'writer trace settings capture')
        require(by_name['max_concurrent_queries_for_user']==dict(name='max_concurrent_queries_for_user',value='1',min='1',max='1'),
            'writer concurrency constraint changed')
        for name,value in TRACE_SETTINGS.items():
            if name in by_name:require(by_name[name]['value']==value,'writer trace setting changed')
        save(self.trace_output/'writer-settings.json',observed)
        result=self.docker('exec','-i',self.ch_entry['id'],'clickhouse-client','--config-file','/run/ch-private/writer.xml',
            '--query_id',self.trace_positive,'--max_result_rows=1','--max_result_bytes=1024','--max_execution_time=5',
            '--receive_timeout=5',data='SELECT 1',timeout=10,label='index-trace-positive')
        require(result.stdout.strip()=='1' and not result.stderr,'positive actual writer query failed')
        self.collect_query_lifecycle('bootstrap')

    def collect_query_lifecycle(self,label):
        self.inspect(self.ch_entry)
        used=sum(p.stat().st_size for folder in ('data','logs') for p in (self.state/'clickhouse'/folder).rglob('*') if p.is_file())
        require(used<2*1024**3 and shutil.disk_usage(self.root).free>65*1024**3,'unchanged ClickHouse disk/completion allowance')
        self.resources['clickhouse_disk']=dict(bytes=used,free_bytes=shutil.disk_usage(self.root).free,limit_bytes=2*1024**3)
        self.record_resources()
        cutoff=getattr(self,'trace_start_through_us',None)
        selected=query_trace_selection(cutoff)
        sql="SYSTEM FLUSH LOGS; SELECT 'count' AS kind,count() AS rows FROM system.query_log WHERE "+selected+" FORMAT JSONEachRow; "
        sql+=("SELECT 'query' AS kind,toString(type) AS event,query_id,initial_query_id,user,initial_user,"
              "toUnixTimestamp64Micro(event_time_microseconds) AS event_us,"
              "toUnixTimestamp64Micro(query_start_time_microseconds) AS start_us,"
              "query_duration_ms AS duration_ms,exception_code,is_initial_query FROM system.query_log WHERE "+selected+
              f" ORDER BY event_time_microseconds,query_id,type LIMIT {TRACE_ROWS+1} FORMAT JSONEachRow")
        result=self.docker('exec','-i',self.ch_entry['id'],'clickhouse-client','--config-file','/run/ch-private/provisioner.xml',
            '--output_format_json_quote_64bit_integers=0',f'--max_result_rows={TRACE_ROWS+1}',f'--max_result_bytes={TRACE_BYTES}',
            '--result_overflow_mode=throw','--max_execution_time=10','--receive_timeout=15','--multiquery',
            data=sql,timeout=30,check=False,label='index-query-lifecycle-'+label)
        require(result.returncode==0 and not result.stderr and len(result.stdout.encode())<=TRACE_BYTES,'query lifecycle flush/export incomplete')
        values=[json.loads(line) for line in result.stdout.splitlines()]
        require(values and set(values[0])=={'kind','rows'} and values[0]['kind']=='count','query lifecycle independent count missing')
        rows=[]
        for value in values[1:]:
            require(value.pop('kind',None)=='query','query lifecycle export row')
            rows.append(value)
        save(self.trace_output/(label+'-rows.json'),rows)
        summary=lifecycle_summary(values[0]['rows'],rows,self.trace_positive)
        if cutoff is not None:
            require(all(row['start_us']<=cutoff for row in rows),'query lifecycle row starts after prefix cutoff')
            summary.update(scope='prefix_started_queries',query_start_through_us=cutoff)
        save(self.trace_output/(label+'-summary.json'),summary)

    def stop_container(self,entry,output=None):
        try:
            if entry['service']=='clickhouse' and hasattr(self,'trace_output') and not getattr(self,'trace_collected',False):
                self.trace_collected=True
                try:
                    require(not self.processes and all(e.get('status')=='stopped-retained' and e.get('pid')==0
                        for e in self.resources['containers'] if e['service'] not in ('clickhouse','database')),
                        'query lifecycle collection requires actual application/client joins')
                    self.collect_query_lifecycle('terminal')
                except BaseException as error:
                    self.resources['cleanup_errors'].append(dict(resource='query-lifecycle-capture',error=type(error).__name__))
                    save(self.trace_output/'terminal-summary.json',dict(status='INCOMPLETE',error_type=type(error).__name__,
                        termination_or_exclusion='NOT_PROVEN'))
        finally:
            # Even interruption or a failed diagnostic write cannot skip the
            # existing owned stop/join or replace the original stage failure.
            terminal=super().stop_container(entry,output)
        return terminal
