"""Exact-rule DuckDB baseline. Optimizations must preserve the reference results."""
import argparse
import csv
import json
import os
from pathlib import Path
import sys
import subprocess
import time
import duckdb

DAY = 86_400_000
WEEK = 7 * DAY

def quote(value):
    return "'" + str(value).replace("'", "''") + "'"

def strict_rows(path, width):
    # newline='' deliberately preserves CR; only LF separates records.
    with open(path, 'rb') as stream:
        next(stream, None)
        for line in stream:
            line = line.removesuffix(b'\n').decode('utf-8')
            fields = line.split(',')
            if len(fields) != width:
                continue
            row = []
            for field in fields:
                if field.startswith('"') and field.endswith('"') and len(field) >= 2:
                    field = field[1:-1]
                if '"' in field:
                    break
                row.append(field)
            else:
                yield row

def identity_map(data):
    parent, size = {}, {}
    def root(x):
        parent.setdefault(x, x)
        size.setdefault(x, 1)
        while x != parent[x]:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for path in sorted((data / 'links').glob('*.csv')):
        for a, b in strict_rows(path, 2):
            if not a or not b:
                continue
            ra, rb = root(a), root(b)
            if ra != rb:
                if size[ra] < size[rb]:
                    ra, rb = rb, ra
                parent[rb] = ra
                size[ra] += size[rb]
    smallest = {}
    for name in parent:
        r = root(name)
        if r not in smallest or name.encode() < smallest[r].encode():
            smallest[r] = name
    return ((name, smallest[root(name)]) for name in parent)

def load_strict(con, data, name, columns, scratch):
    width = len(columns)
    parser = Path(__file__).with_name('parse_csv')
    if parser.is_file():
        paths = sorted((data / name).glob('*.csv'))
        normalized = scratch / (name + '-normalized.csv')
        con.execute(f'CREATE TABLE {name} (' + ','.join(f'{c} VARCHAR' for c in columns) + ')')
        if paths:
            subprocess.run([str(parser.resolve()), str(width), str(normalized), *map(str,paths)], check=True)
            if normalized.stat().st_size:
                schema = '{' + ','.join(quote(c) + ": 'VARCHAR'" for c in columns) + '}'
                con.execute(f"INSERT INTO {name} SELECT * FROM read_csv({quote(normalized)}, header=false, auto_detect=false, columns={schema}, delim=',', quote='\"', escape='\"', new_line='\\n', nullstr='', strict_mode=true)")
            normalized.unlink()
        return
    pattern = ','.join(['(?:"[^",]*"|[^",]*)'] * width)
    paths = sorted((data / name).glob('*.csv'))
    if not paths:
        con.execute(f'CREATE TABLE {name} (' + ','.join(f'{c} VARCHAR' for c in columns) + ')')
        return
    fields = ','.join(f"NULLIF(CASE WHEN starts_with(f[{i+1}], '\"') THEN substr(f[{i+1}], 2, length(f[{i+1}])-2) ELSE f[{i+1}] END, '') AS {c}" for i,c in enumerate(columns))
    # read_text avoids stock CSV readers repairing CRLF or stray quotes.
    con.execute(f'CREATE TABLE {name} (' + ','.join(f'{c} VARCHAR' for c in columns) + ')')
    for path in paths:
      con.execute(f'''INSERT INTO {name}
      SELECT {fields} FROM (
        SELECT string_split(line, ',') f FROM (
          SELECT unnest(string_split(content, chr(10))) line,
            generate_subscripts(string_split(content, chr(10)), 1) n FROM read_text({quote(path)})
        ) WHERE n > 1 AND regexp_full_match(line, {quote(pattern)})
      )''')

def run(data, output, scratch):
    started = time.perf_counter()
    data, output, scratch = map(Path, (data, output, scratch))
    scratch.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={'threads':int(os.environ.get('NCPU','8')),
        'memory_limit':os.environ.get('MEMORY_LIMIT','16GB'),
        'temp_directory':str(scratch / 'duckdb_tmp'), 'preserve_insertion_order':False})
    def stage(name, query):
        t = time.perf_counter(); con.execute(query)
        print(f'{name}: {time.perf_counter()-t:.3f}s', file=sys.stderr)
    con.execute("SET TimeZone='UTC'")
    con.execute(r'''CREATE MACRO timestamp_ms(s) AS CASE
      WHEN regexp_full_match(s, '[0-9]+') THEN try_cast(s AS BIGINT)
      WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])')
      THEN epoch_ms(try_cast(s AS TIMESTAMPTZ)) ELSE NULL END''')
    manifest = json.loads((data / 'manifest.json').read_text())
    lo, hi = con.execute('SELECT timestamp_ms(?), timestamp_ms(?)', [manifest['window_start'],manifest['window_end']]).fetchone()
    if lo is None or hi is None or lo >= hi:
        raise ValueError('invalid manifest window')
    con.execute('CREATE TABLE identities(id VARCHAR, person VARCHAR)')
    mapping = scratch / 'identities.csv'
    with mapping.open('w',newline='',encoding='utf-8') as f:
        w=csv.writer(f); w.writerow(['id','person']); w.writerows(identity_map(data))
    con.execute(f"INSERT INTO identities SELECT * FROM read_csv({quote(mapping)}, header=true, all_varchar=true)")
    for table, columns in [('events',['event_id','user_id','ts','ingest_ts','event_type','product_id','price_cents']), ('clicks',['click_id','user_id','campaign_id','ts'])]:
        t=time.perf_counter();load_strict(con,data,table,columns,scratch);print(f'read {table}: {time.perf_counter()-t:.3f}s',file=sys.stderr)
    for table in ['products','campaigns']:
        con.execute(f'CREATE TABLE {table} AS SELECT * FROM read_csv({quote(data / (table+".csv"))}, header=true, all_varchar=true)')
    stage('dedup', '''CREATE TABLE dedup AS SELECT * FROM events WHERE event_id IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts)=1; DROP TABLE events''')
    stage('validate',f'''CREATE TABLE valid AS
      WITH parsed AS (SELECT *, timestamp_ms(ts) t, timestamp_ms(ingest_ts) ingest,
        CASE WHEN event_type='purchase' AND regexp_full_match(price_cents,'[0-9]+') THEN try_cast(price_cents AS BIGINT) ELSE 0 END cents FROM dedup)
      SELECT coalesce(i.person,e.user_id) person,t,cents,coalesce(p.category,'unknown') category
      FROM parsed e LEFT JOIN identities i ON e.user_id=i.id LEFT JOIN products p USING(product_id)
      WHERE e.user_id IS NOT NULL AND product_id IS NOT NULL AND t >= {lo} AND t < {hi}
        AND event_type IN ('view','cart','purchase') AND (event_type!='purchase' OR cents BETWEEN 1 AND 10000000)
        AND ingest-t <= {DAY}; DROP TABLE dedup''')
    stage('sessions', '''CREATE TABLE session_events AS
      WITH gaps AS (SELECT *, t-lag(t) OVER (PARTITION BY person ORDER BY t) gap FROM valid)
      SELECT *,sum(CASE WHEN gap IS NULL OR gap>1800000 THEN 1 ELSE 0 END)
        OVER (PARTITION BY person ORDER BY t ROWS UNBOUNDED PRECEDING) sid FROM gaps;
      CREATE TABLE sessions AS SELECT person,sid,min(t) start_ms,
        CAST(epoch_ms(min(t)) AS DATE) AS day,count(*)::BIGINT events,sum(cents)::BIGINT revenue_cents
        FROM session_events GROUP BY person,sid; DROP TABLE valid''')
    stage('clicks',f'''CREATE TABLE valid_clicks AS
      WITH d AS (SELECT * FROM clicks WHERE click_id IS NOT NULL QUALIFY row_number() OVER (PARTITION BY click_id)=1),
      p AS (SELECT *,timestamp_ms(ts) t FROM d)
      SELECT coalesce(i.person,c.user_id) person,click_id,campaign_id,t
      FROM p c JOIN campaigns USING(campaign_id) LEFT JOIN identities i ON c.user_id=i.id
      WHERE user_id IS NOT NULL AND t >= {lo-WEEK} AND t < {hi}; DROP TABLE clicks;
      CREATE TABLE credit_clicks AS SELECT * FROM valid_clicks
        QUALIFY row_number() OVER (PARTITION BY person,t ORDER BY click_id)=1''')
    stage('credit',f'''CREATE TABLE credited AS SELECT s.*,
        CASE WHEN s.start_ms-c.t <= {WEEK} THEN c.campaign_id ELSE 'organic' END campaign_id
      FROM sessions s ASOF LEFT JOIN credit_clicks c ON s.person=c.person AND s.start_ms>=c.t''')
    stage('assists',f'''CREATE TABLE intervals AS
      WITH distinct_times AS (SELECT DISTINCT person,campaign_id,t FROM valid_clicks),
      gaps AS (SELECT *,lag(t) OVER (PARTITION BY person,campaign_id ORDER BY t) prev FROM distinct_times),
      groups AS (SELECT *,sum(CASE WHEN prev IS NULL OR t-prev>{WEEK} THEN 1 ELSE 0 END)
        OVER (PARTITION BY person,campaign_id ORDER BY t ROWS UNBOUNDED PRECEDING) grp FROM gaps)
      SELECT person,campaign_id,min(t) first_ms,max(t)+{WEEK} last_ms FROM groups GROUP BY person,campaign_id,grp;
      CREATE TABLE assists AS SELECT campaign_id,day,count(*)::BIGINT assisted_sessions
      FROM sessions s JOIN intervals i ON s.person=i.person AND s.start_ms>=i.first_ms AND s.start_ms<=i.last_ms
      GROUP BY campaign_id,day''')
    stage('aggregate', '''CREATE TABLE user_daily AS
      WITH category_totals AS (SELECT e.person,s.day,e.category,sum(e.cents) revenue
        FROM session_events e JOIN sessions s USING(person,sid) WHERE e.cents>0 GROUP BY e.person,s.day,e.category),
      top AS (SELECT person,day,category FROM category_totals QUALIFY row_number() OVER (PARTITION BY person,day ORDER BY revenue DESC,category)=1),
      daily AS (SELECT person,day,count(*)::BIGINT sessions,sum(events)::BIGINT events,sum(revenue_cents)::BIGINT revenue_cents FROM sessions GROUP BY person,day)
      SELECT person user_id,day,sessions,events,revenue_cents,category top_category FROM daily LEFT JOIN top USING(person,day);
      CREATE TABLE campaign_daily AS
      WITH credits AS (SELECT campaign_id,day,count(*)::BIGINT sessions,
        count(*) FILTER (WHERE revenue_cents>0)::BIGINT purchasing_sessions,sum(revenue_cents)::BIGINT revenue_cents
        FROM credited GROUP BY campaign_id,day),
      combined AS (SELECT coalesce(c.campaign_id,a.campaign_id) campaign_id,coalesce(c.day,a.day) AS day,
        coalesce(sessions,0)::BIGINT sessions,coalesce(purchasing_sessions,0)::BIGINT purchasing_sessions,
        coalesce(revenue_cents,0)::BIGINT revenue_cents,coalesce(assisted_sessions,0)::BIGINT assisted_sessions
        FROM credits c FULL OUTER JOIN assists a USING(campaign_id,day))
      SELECT campaign_id,CASE WHEN campaign_id='organic' THEN 'organic' ELSE channel END channel,
        day,sessions,purchasing_sessions,revenue_cents,assisted_sessions FROM combined LEFT JOIN campaigns USING(campaign_id)''')
    for table in ['user_daily','campaign_daily']:
        folder = output / table;folder.mkdir(parents=True,exist_ok=True)
        # Only remove our own previous output; refuse unrelated stale Parquet files.
        target=folder/'part-0.parquet'
        if any(p!=target for p in folder.glob('*.parquet')):
            raise ValueError(f'use a clean output directory: {folder}')
        con.execute(f'COPY {table} TO {quote(target)} (FORMAT PARQUET, COMPRESSION ZSTD)')
    con.close()
    print(f'total: {time.perf_counter()-started:.3f}s',file=sys.stderr)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('data',nargs='?',default=os.environ.get('DATA_DIR','/data'));parser.add_argument('output',nargs='?',default=os.environ.get('OUTPUT_DIR','/output'));parser.add_argument('--scratch',default=os.environ.get('SCRATCH_DIR','/scratch'))
    args=parser.parse_args();run(args.data,args.output,args.scratch)
