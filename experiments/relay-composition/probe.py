"""Small isolated research model; no product service, network or capacity claim."""
from __future__ import annotations

import argparse
from collections import Counter
import copy
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'experiments/identity-contract'))
from signed_fixture import sign, verified  # Existing independent crypto research helper.


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':')).encode()


def fixture():
    events = {
        'a1': sign(1, 10, 1, [['t', 'nostr'], ['t', 'nostr'], ['t', 'rust']], 'alpha'),
        'a2': sign(1, 20, 1, [['t', 'nostr']], 'beta'),
        'b1': sign(2, 30, 7, [], '+'),
        'private': sign(3, 40, 1, [], 'private-only'),
        'claim': sign(3, 50, 1, [], 'imported-only-presence'),
        'old': sign(1, 60, 30023, [['d', 'article']], 'old'),
        'new': sign(1, 70, 30023, [['d', 'article']], 'new'),
        'expires': sign(2, 80, 1, [['expiration', '130']], 'é'),
        'unscored': sign(3, 90, 1, [], 'unknown author score'),
    }
    # Entries are modeled evidence, not assertions of actual network observation.
    actions = [
        ('a1', 'R', 'public', 'local', 100),
        ('a1', 'R', 'public', 'local', 100),
        ('a2', 'R', 'public', 'local', 101),
        ('b1', 'R', 'public', 'local', 102),
        ('private', 'R', 'private', 'local', 103),
        ('claim', 'R', 'public', 'claim', 104),
        ('old', 'R', 'public', 'local', 105),
        ('expires', 'R', 'public', 'local', 106),
        ('new', 'R2', 'public', 'local', 107),
        ('unscored', 'R', 'public', 'local', 108),
        ('a1', 'R', 'public', 'local', 99),  # Earlier timing repair, same membership.
    ]
    return events, actions


def coordinate(event):
    if 30000 <= event['kind'] < 40000:
        return (event['kind'], event['pubkey'], next(t[1] for t in event['tags'] if t[0] == 'd'))
    return None


class Incremental:
    def __init__(self, events, visible, begin=0):
        self.events, self.visible, self.begin = events, visible, begin
        self.public, self.witnesses, self.members = set(), set(), set()
        self.winners, self.authors, self.kinds, self.topics = {}, Counter(), Counter(), Counter()
        self.squares = self.content_bytes = 0

    def eligible(self, label, now):
        event = self.events[label]
        if label not in self.witnesses or not self.begin <= event['created_at'] < 100:
            return False
        if not self.visible:
            return True
        coord = coordinate(event)
        if coord and self.winners.get(coord) != label:
            return False
        expiry = next((int(t[1]) for t in event['tags'] if t[0] == 'expiration'), None)
        return expiry is None or now < expiry

    def reconcile(self, labels, now):
        for label in labels:
            keep = self.eligible(label, now)
            if keep == (label in self.members):
                continue
            event = self.events[label]
            delta = 1 if keep else -1
            author = event['pubkey']
            n = self.authors[author]
            self.squares += 2*n*delta + delta*delta
            self.authors[author] += delta
            if not self.authors[author]:
                del self.authors[author]
            self.kinds[str(event['kind'])] += delta
            if not self.kinds[str(event['kind'])]:
                del self.kinds[str(event['kind'])]
            for topic in {t[1] for t in event['tags'] if len(t)>1 and t[0]=='t' and t[1]}:
                self.topics[topic] += delta
                if not self.topics[topic]:
                    del self.topics[topic]
            self.content_bytes += delta*len(event['content'].encode())
            (self.members.add if keep else self.members.remove)(label)

    def apply(self, action, now=120):
        label, endpoint, visibility, origin, _ = action
        affected = {label}
        if visibility == 'public':
            self.public.add(label)
            event = self.events[label]
            coord = coordinate(event)
            if coord:
                previous = self.winners.get(coord)
                if previous:
                    affected.add(previous)
                candidates = [label] + ([previous] if previous else [])
                self.winners[coord] = min(candidates, key=lambda x:(-self.events[x]['created_at'], self.events[x]['id']))
            if endpoint == 'R' and origin == 'local':
                self.witnesses.add(label)
        self.reconcile(affected, now)

    def summary(self):
        return dict(members=sorted(self.members), events=len(self.members), authors=len(self.authors),
                    author_counts=dict(self.authors), kinds=dict(self.kinds), topics=dict(self.topics),
                    content_bytes=self.content_bytes, sum_author_squares=self.squares)


def sql_oracle(events, actions, visible, now=120, begin=0):
    # Rebuild independently from source rows; no Incremental helpers or state.
    db = sqlite3.connect(':memory:')
    db.executescript('''CREATE TABLE e(label TEXT,id TEXT,author TEXT,kind INT,created INT,d TEXT,expiry INT,content TEXT);
        CREATE TABLE w(label TEXT,endpoint TEXT,visibility TEXT,origin TEXT);
        CREATE TABLE topics(label TEXT,topic TEXT);''')
    for label, e in events.items():
        d = next((t[1] for t in e['tags'] if len(t)>1 and t[0]=='d'), '')
        expiry = next((int(t[1]) for t in e['tags'] if len(t)>1 and t[0]=='expiration'), None)
        db.execute('INSERT INTO e VALUES(?,?,?,?,?,?,?,?)', (label,e['id'],e['pubkey'],e['kind'],e['created_at'],d,expiry,e['content']))
        db.executemany('INSERT INTO topics VALUES(?,?)', [(label,t[1]) for t in e['tags'] if len(t)>1 and t[0]=='t' and t[1]])
    db.executemany('INSERT INTO w VALUES(?,?,?,?)', [x[:4] for x in actions])
    db.execute('''CREATE TEMP TABLE selected AS SELECT e.* FROM e
        WHERE created>=? AND created<100
        AND EXISTS(SELECT 1 FROM w WHERE w.label=e.label AND endpoint='R' AND visibility='public' AND origin='local')
        AND (?=0 OR ((expiry IS NULL OR expiry>?) AND NOT EXISTS(
          SELECT 1 FROM e AS newer WHERE e.kind>=30000 AND e.kind<40000
          AND newer.kind=e.kind AND newer.author=e.author AND newer.d=e.d
          AND (newer.created>e.created OR (newer.created=e.created AND newer.id<e.id))
          AND EXISTS(SELECT 1 FROM w WHERE w.label=newer.label AND visibility='public'))))''', (begin,int(visible),now))
    ac = dict(db.execute('SELECT author,count(*) FROM selected GROUP BY author'))
    result = dict(members=[r[0] for r in db.execute('SELECT label FROM selected ORDER BY label')],
        events=db.execute('SELECT count(*) FROM selected').fetchone()[0], authors=len(ac),author_counts=ac,
        kinds=dict(db.execute('SELECT CAST(kind AS TEXT),count(*) FROM selected GROUP BY kind')),
        topics=dict(db.execute('''SELECT topic,count(*) FROM
            (SELECT DISTINCT topics.label,topic FROM topics JOIN selected USING(label)) GROUP BY topic''')),
        content_bytes=db.execute('SELECT COALESCE(sum(length(CAST(content AS BLOB))),0) FROM selected').fetchone()[0],
        sum_author_squares=sum(n*n for n in ac.values()))
    db.close()
    return result


def bands(summary, ranks):
    result = {k:dict(authors=0,events=0) for k in ('0','1-25','26-50','51-75','76-100','unknown')}
    for author, n in summary['author_counts'].items():
        rank = ranks.get(author)
        key = 'unknown' if rank is None else '0' if rank == 0 else ('1-25','26-50','51-75','76-100')[(rank-1)//25]
        result[key]['authors'] += 1
        result[key]['events'] += n
    return result


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    events, actions=fixture()
    assert all(verified(e) for e in events.values())
    broken=copy.deepcopy(events['a1']);broken['content']+='changed';assert not verified(broken)
    models={name:Incremental(events,visible) for name,visible in [('history',False),('visible',True)]}
    checked=0
    for cut, action in enumerate(actions,1):
        for name, model in models.items():
            model.apply(action)
            assert model.summary()==sql_oracle(events,actions[:cut],name=='visible')
            stable=copy.deepcopy(model.summary());model.apply(action)
            assert model.summary()==stable
            checked+=1
    before={name:m.summary() for name,m in models.items()}
    assert before['history']['events']==6 and before['history']['authors']==3
    assert before['history']['sum_author_squares']==14
    assert before['visible']['events']==5 and before['visible']['sum_author_squares']==9
    assert before['history']['topics']=={'nostr':2,'rust':1}
    for name,model in models.items():
        model.reconcile(set(model.members),130)
        assert model.summary()==sql_oracle(events,actions,name=='visible',130)
    after={name:m.summary() for name,m in models.items()}
    assert after['visible']['events']==4 and after['visible']['sum_author_squares']==6
    assert after['history']==before['history']
    windowed=Incremental(events,False,20)
    for action in actions:windowed.apply(action)
    assert windowed.summary()==sql_oracle(events,actions,False,begin=20)
    ranks={events['a1']['pubkey']:80,events['b1']['pubkey']:0}
    banded=bands(after['visible'],ranks)
    assert banded['76-100']==dict(authors=1,events=2)
    assert banded['0']==dict(authors=1,events=1) and banded['unknown']==dict(authors=1,events=1)
    ranks[events['a1']['pubkey']]=20
    assert bands(after['visible'],ranks)['1-25']==dict(authors=1,events=2)
    # Identical local evidence cannot distinguish two possible later remote states.
    possible_remote_states=[set(before['history']['members']),set()]
    assert len(possible_remote_states[0])!=len(possible_remote_states[1])
    definition=dict(schema='bigbrotr-definition/1',id='relay.composition/1',version=1,
                    component='events',unit='events',population='witness-history/1',
                    formula='cardinality of distinct verified event IDs with eligible local witnesses',
                    claim_class='deterministic-aggregate',remote_inventory='unknown')
    definition_bytes=encoded(definition); dh=digest(definition_bytes)
    context=dict(schema='bigbrotr-context/1',kind=30166,d='wss://relay.invalid',
                 definition_ref=dict(id=definition['id'],version=1,sha256=dh,url=f'https://data.invalid/{dh}.json'),
                 assertion=dict(api_only=True,component='events',value='6',unit='events',population='witness-history/1'),
                 limitations=['synthetic source fixture','no present remote inventory inference'])
    context_bytes=encoded(context);ch=digest(context_bytes)
    event=sign(4,200,30166,[['d','wss://relay.invalid'],['bigbrotr:meta','1',ch,f'https://data.invalid/{ch}.json'],
                         ['L','bigbrotr.assertion-basis/1'],['l','derived-fact','bigbrotr.assertion-basis/1']], '')
    assert verified(event) and event['tags'][1][2]==digest(context_bytes)
    assert digest(context_bytes+b' ')!=ch
    assert not any(t[0] in ('k','comment_cnt','rank') for t in event['tags'])
    wrong_definition=dict(definition,id='another.definition/1')
    wrong_hash=digest(encoded(wrong_definition))
    wrong_definition_ref=dict(context['definition_ref'],sha256=wrong_hash,
                              url=f'https://data.invalid/{wrong_hash}.json')
    assert digest(encoded(wrong_definition))==wrong_definition_ref['sha256']
    assert wrong_definition['id']!=wrong_definition_ref['id']
    hist=before['history']
    raw_deliveries=sum(ep=='R' and scope=='public' and origin=='local' for _,ep,scope,origin,_ in actions)
    include_claims={label for label,ep,scope,_,_ in actions if ep=='R' and scope=='public'}
    include_private={label for label,ep,_,origin,_ in actions if ep=='R' and origin=='local'}
    sum_partition_distinct=sum(len({events[x]['pubkey'] for x in hist['members']
        if low<=events[x]['created_at']<high}) for low,high in [(0,75),(75,100)])
    repeated_topics=sum(t==['t','nostr'] for x in hist['members'] for t in events[x]['tags'])
    zero_as_absent=bands(after['visible'],{a:r for a,r in ranks.items() if r})
    negative_controls=dict(duplicate_deliveries=raw_deliveries!=hist['events'],
        imported_claim=len(include_claims)!=hist['events'],private_evidence=len(include_private)!=hist['events'],
        distinct_partition_sum=sum_partition_distinct!=hist['authors'],
        duplicate_topic=repeated_topics!=hist['topics']['nostr'],
        no_replacement=hist['events']!=before['visible']['events'],
        no_expiry=before['visible']['events']!=after['visible']['events'],
        subtract_square_only=before['visible']['sum_author_squares']-1!=after['visible']['sum_author_squares'],
        zero_equals_unknown=zero_as_absent['0']!=banded['0'],
        wrong_definition_id=(digest(encoded(wrong_definition))==wrong_definition_ref['sha256']
                             and wrong_definition['id']!=wrong_definition_ref['id']),
        tampered_context=digest(context_bytes+b' ')!=ch,
        corrupted_signed_content=not verified(broken),
        remote_states_not_identifiable=len(possible_remote_states[0])!=len(possible_remote_states[1]))
    assert all(negative_controls.values())
    payload=dict(events=events,actions=actions,definition=definition,context=context,published_event=event,
                 wrong_definition_binding=dict(definition=wrong_definition,reference=wrong_definition_ref))
    (args.output/'fixture.json').write_bytes(encoded(payload)+b'\n')
    (args.output/'definition.json').write_bytes(definition_bytes)
    (args.output/'context.json').write_bytes(context_bytes)
    result=dict(status='PASS',source_sha256=digest(Path(__file__).read_bytes()),
                signed_fixture_sha256=digest((args.output/'fixture.json').read_bytes()),
                signed_source_events=len(events),signed_extension_events=1,
                incremental_sql_cut_comparisons=checked,duplicate_replay_checks=checked,
                zero_arrival_expiry_comparisons=2,negative_controls=negative_controls,
                before_expiry=before,after_expiry=after,curator_bands=banded,
                definition_bytes=len(definition_bytes),context_bytes=len(context_bytes),
                definition_sha256=dh,context_sha256=ch,
                python_secp256k1=importlib.metadata.version('secp256k1'),sqlite=sqlite3.sqlite_version,
                scope='In-memory Python incremental counters versus fresh SQLite set/group oracle and static expected counts; real BIP340 signatures, existing library and independent group-equation verification. Synthetic metadata subset, not a complete production schema or interoperable hosted context.',
                not_executed=['Rust product','relay/network/HTTP','persistent crash recovery','capacity or timing benchmark','independent external client','full protocol conformance'])
    (args.output/'result.json').write_bytes(json.dumps(result,indent=2,sort_keys=True).encode()+b'\n')
    print(json.dumps({k:result[k] for k in ['status','signed_source_events','incremental_sql_cut_comparisons','duplicate_replay_checks','negative_controls']},indent=2))


if __name__=='__main__':main()
