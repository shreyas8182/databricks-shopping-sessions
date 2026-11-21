"""Small-data reference: independent parsing, graph traversal, sessions and click scans.
Not the production pipeline; deliberately favors obvious semantics over speed.
"""
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import re

DAY=86400000
WEEK=7*DAY
EPOCH=datetime(1970,1,1,tzinfo=timezone.utc)
ISO=re.compile(r'([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|[+-][0-9]{2}:[0-9]{2})')

def ts(value):
    if re.fullmatch('[0-9]+',value):
        # Avoid Python's large-integer digit limit while accepting leading zeros.
        digits=value.lstrip('0') or '0'
        return int(digits) if len(digits)<19 else None
    m=ISO.fullmatch(value)
    if not m:return None
    y,mo,d,h,mi,s=map(int,m.groups()[:6]);z=m[7]
    try:
        offset=0
        if z!='Z':
            oh,om=int(z[1:3]),int(z[4:6])
            if oh>23 or om>59:return None
            offset=(oh*60+om)*(1 if z[0]=='+' else -1)
        dt=datetime(y,mo,d,h,mi,s,tzinfo=timezone.utc)-timedelta(minutes=offset)
        delta=dt-EPOCH
        return delta.days*DAY+delta.seconds*1000
    except (ValueError,OverflowError):return None

def rows(path,width):
    # Binary LF splitting does not repair CRs anywhere in fields.
    for line in Path(path).read_bytes().split(b'\n')[1:]:
        fields=line.decode('utf-8').split(',')
        if len(fields)!=width:continue
        result=[]
        for f in fields:
            if '"' in f:
                if len(f)<2 or f[0]!='"' or f[-1]!='"' or '"' in f[1:-1]:break
                f=f[1:-1]
            result.append(f)
        else:yield result

def solve(data):
    data=Path(data);manifest=json.loads((data/'manifest.json').read_text());lo=ts(manifest['window_start']);hi=ts(manifest['window_end'])
    graph=defaultdict(set)
    for path in (data/'links').glob('*.csv'):
        for a,b in rows(path,2):
            if a and b:graph[a].add(b);graph[b].add(a)
    canonical={}
    for node in graph:
        if node in canonical:continue
        seen={node};stack=[node]
        while stack:
            for neighbor in graph[stack.pop()]:
                if neighbor not in seen:seen.add(neighbor);stack.append(neighbor)
        name=min(seen,key=lambda s:s.encode('utf-8'))
        canonical.update((x,name) for x in seen)
    products=dict(rows(data/'products.csv',2));campaigns=dict(rows(data/'campaigns.csv',2))
    dedup={}
    for path in (data/'events').glob('*.csv'):
        for e in rows(path,7):
            if e[0] and (e[0] not in dedup or e[3]<dedup[e[0]][3]):dedup[e[0]]=e
    events=defaultdict(list)
    for _,u,t,ing,kind,product,price in dedup.values():
        t=ts(t)
        if not u or not product or t is None or not lo<=t<hi or kind not in ('view','cart','purchase'):continue
        cents=0
        if kind=='purchase':
            if not re.fullmatch('[0-9]+',price):continue
            digits=price.lstrip('0') or '0'
            if len(digits)>8:continue
            cents=int(digits)
            if not 1<=cents<=10000000:continue
        if ts(ing)-t>DAY:continue
        events[canonical.get(u,u)].append((t,cents,products.get(product,'unknown')))
    clicks=defaultdict(list);seen_clicks=set()
    for path in (data/'clicks').glob('*.csv'):
        for k,u,c,t in rows(path,4):
            if not k or k in seen_clicks:continue
            seen_clicks.add(k);t=ts(t)
            if u and c in campaigns and t is not None and lo-WEEK<=t<hi:
                clicks[canonical.get(u,u)].append((t,k,c))
    ud={};cd={}
    for person,ev in events.items():
        ev.sort();sessions=[]
        for event in ev:
            if not sessions or event[0]-sessions[-1][-1][0]>1800000:sessions.append([])
            sessions[-1].append(event)
        for session in sessions:
            start=session[0][0];day=(EPOCH+timedelta(milliseconds=start)).date();revenue=sum(e[1] for e in session)
            user=ud.setdefault((person,day),[0,0,0,defaultdict(int)])
            user[0]+=1;user[1]+=len(session);user[2]+=revenue
            for _,cents,category in session:
                if cents:user[3][category]+=cents
            qualifying=[c for c in clicks[person] if 0<=start-c[0]<=WEEK]
            credited=min(qualifying,key=lambda c:(-c[0],c[1].encode()))[2] if qualifying else 'organic'
            campaign=cd.setdefault((credited,day),[0,0,0,0]);campaign[0]+=1;campaign[1]+=int(revenue>0);campaign[2]+=revenue
            for c in {c[2] for c in qualifying}:cd.setdefault((c,day),[0,0,0,0])[3]+=1
    users=[]
    for (u,day),(n,e,r,categories) in ud.items():
        top=min(categories,key=lambda c:(-categories[c],c.encode())) if categories else None
        users.append((u,day,n,e,r,top))
    return {'user_daily':users,'campaign_daily':[(c,'organic' if c=='organic' else campaigns[c],day,*values) for (c,day),values in cd.items()]}
