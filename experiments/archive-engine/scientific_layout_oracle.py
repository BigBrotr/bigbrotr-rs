#!/usr/bin/env python3
"""Independent closed-DDL oracle; never used to initialize or migrate a catalog.

Pinned DuckLake1.0 serializes pre-order field IDs, child name `element`, nested
nullable fields and literal NULL defaults. Compare declaration-derived rows with
both the compiled inspection fixture and separately collected actual PG rows.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
TYPES = {'BLOB':'blob', 'USMALLINT':'uint16', 'UINTEGER':'uint32',
         'UBIGINT':'uint64', 'BIGINT':'int64', 'VARCHAR':'varchar'}

# Frozen independent query from scientific_guard_cases.layout_cases (91af8a9).
# Kept separate from the product wire projection and its overflow sentinel.
STRUCTURAL_SQL = '''WITH objects(kind,ns,name,definition) AS (
SELECT 'table',n.nspname::text,c.relname::text,ROW(c.relkind,c.relpersistence,c.relrowsecurity,c.relforcerowsecurity,c.relispartition,c.reloptions)::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind<>'i'
UNION ALL SELECT 'column',n.nspname,c.relname||'.'||a.attnum,ROW(a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attisdropped,a.attidentity,a.attgenerated,a.attcollation::regcollation::text,a.attstorage,a.attcompression,pg_get_expr(d.adbin,d.adrelid))::text FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attnum>0
UNION ALL SELECT 'constraint',n.nspname,c.relname||'.'||p.conname,pg_get_constraintdef(p.oid,false) FROM pg_constraint p JOIN pg_class c ON c.oid=p.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
UNION ALL SELECT 'index',n.nspname,c.relname,ROW(pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready,i.indislive)::text FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace
UNION ALL SELECT 'trigger',n.nspname,c.relname||'.'||t.tgname,ROW(pg_get_triggerdef(t.oid,false),t.tgenabled,t.tgisinternal)::text FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
UNION ALL SELECT 'function',n.nspname,p.proname||'('||pg_get_function_identity_arguments(p.oid)||')',pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.prokind IN ('f','p')
UNION ALL SELECT 'rule',n.nspname,c.relname||'.'||r.rulename,pg_get_ruledef(r.oid,false) FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class JOIN pg_namespace n ON n.oid=c.relnamespace
) SELECT kind,ns,name,definition FROM objects WHERE ns IN ('public','bigbrotr_archive') ORDER BY kind COLLATE "C",ns COLLATE "C",name COLLATE "C"'''


def structural_checksum(rows):
    digest=hashlib.sha256(b'bigbrotr/catalog-guard-layout/1\0')
    for row in rows:
        for field in row:
            encoded=field.encode('utf-8')
            digest.update(len(encoded).to_bytes(8,'big'));digest.update(encoded)
    return digest.hexdigest()


def structural_statement():
    return ('SET search_path=pg_catalog; SELECT json_build_array(kind,ns,name,definition) FROM ('+
        STRUCTURAL_SQL+') AS captured_layout ORDER BY kind COLLATE "C",ns COLLATE "C",name COLLATE "C" LIMIT 1025')

class Declaration:
    def __init__(self, text):
        self.tokens = re.findall(r'[A-Za-z_]+|[(),\[\]]', text)
        assert ''.join(self.tokens) == re.sub(r'\s+', '', text)
        self.pos = 0
    def take(self, expected=None):
        token = self.tokens[self.pos]
        self.pos += 1
        assert expected is None or token == expected, (expected, token)
        return token
    def at(self, value):
        return self.pos < len(self.tokens) and self.tokens[self.pos] == value
    def type(self):
        token = self.take()
        if token == 'STRUCT':
            self.take('(')
            node = ('struct', self.fields(nested=True))
            self.take(')')
        else:
            node = (TYPES[token], [])
        while self.at('['):
            self.take('['); self.take(']')
            node = ('list', [('element', node, True)])
        return node
    def fields(self, nested=False):
        fields = []
        while True:
            name, node = self.take(), self.type()
            nullable = True
            if self.at('NOT'):
                assert not nested
                self.take('NOT'); self.take('NULL'); nullable = False
            fields.append((name, node, nullable))
            if not self.at(','): break
            self.take(',')
        return fields
    def columns(self):
        fields = self.fields()
        assert self.pos == len(self.tokens)
        rows = []
        def visit(fields, parent):
            for name, (kind, children), nullable in fields:
                field = len(rows) + 1
                rows.append([field, name, kind, parent, nullable])
                visit(children, field)
        visit(fields, None)
        return rows

def expected():
    path = ROOT/'contracts/archive/v1/schema.json'
    schema = json.loads(path.read_text())
    assert schema['format'] == 'bigbrotr-archive-schema/1'
    return {'format':'bigbrotr-archive-native-layout/1',
            'schema_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
            'tables':{t['name']:Declaration(t['columns']).columns() for t in schema['tables']}}

def observed(raw, wanted):
    tables = {t['table_id']:t['table_name'] for t in raw['ducklake_table']}
    actual = {name:[] for name in tables.values()}
    for c in sorted(raw['ducklake_column'], key=lambda c:c['column_id']):
        assert c['begin_snapshot'] == 1 and c['end_snapshot'] is None
        assert c['column_order'] == c['column_id'] and c['initial_default'] is None
        assert (c['default_value'],c['default_value_type'],c['default_value_dialect']) == ('NULL','' if c['column_type'] in ('struct','list') else 'literal','duckdb')
        actual[tables[c['table_id']]].append([c['column_id'], c['column_name'], c['column_type'], c['parent_column'], c['nulls_allowed']])
    assert actual == wanted['tables']

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--write',action='store_true')
    parser.add_argument('--observed',type=Path)
    args=parser.parse_args()
    result=expected()
    fixture=ROOT/'contracts/archive/v1/native-layout.json'
    if args.write:
        # One field per line keeps all nested identities reviewable.
        head={k:v for k,v in result.items() if k != 'tables'}
        lines=json.dumps(head,indent=2).splitlines()[:-1]
        lines[-1] += ','
        lines.append('  "tables": {')
        for i,(name,columns) in enumerate(result['tables'].items()):
            lines.append('    '+json.dumps(name)+': [')
            lines.extend('      '+json.dumps(c)+(',' if j+1<len(columns) else '') for j,c in enumerate(columns))
            lines.append('    ]'+(',' if i+1<len(result['tables']) else ''))
        fixture.write_text('\n'.join(lines+['  }','}'])+'\n')
    assert json.loads(fixture.read_text()) == result
    if args.observed: observed(json.loads(args.observed.read_text()),result)
    print(json.dumps({'status':'PASS','tables':len(result['tables']),'columns':sum(map(len,result['tables'].values())),'actual_pg_compared':bool(args.observed)}))
