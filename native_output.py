"""Run the compiled shopper-local aggregation and encode its daily rows as Parquet."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

import duckdb


def run_native(executable, data, output, scratch):
    scratch.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((data / 'manifest.json').read_text())
    threads = os.environ.get('NCPU', '8')
    with tempfile.TemporaryDirectory(dir=scratch) as temp:
        subprocess.run([str(executable), str(data), temp, manifest['window_start'],
                        manifest['window_end'], threads], check=True)
        with duckdb.connect(config={'threads': int(threads),
                                    'temp_directory': temp,
                                    'memory_limit': os.environ.get('MEMORY_LIMIT', '16GB')}) as con:
            write_daily(con, Path(temp), output, 'user', ['user_id', 'd', 'sessions', 'events',
                        'revenue_cents', 'top_category'])
            write_daily(con, Path(temp), output, 'campaign', ['campaign_id', 'channel', 'd',
                        'sessions', 'purchasing_sessions', 'revenue_cents', 'assisted_sessions'])


def write_daily(con, temp, output, kind, names):
    strings = {'user_id', 'top_category', 'campaign_id', 'channel'}
    types = {name: ('VARCHAR' if name in strings else 'INTEGER' if name == 'd' else 'BIGINT')
             for name in names}
    paths = [str(p) for p in temp.glob(f'{kind}-*.csv') if p.stat().st_size]
    if paths:
        # The SQL reader exposes explicit LF handling for CR inside quoted IDs.
        con.sql("SELECT * FROM read_csv($paths, columns=$types, header=false, "
                "auto_detect=false, new_line='\\n', delim=',')",
                params={'paths': paths, 'types': types}).create_view('partial')
    else:
        con.execute('CREATE VIEW partial AS SELECT ' + ','.join(
            f'NULL::{sql_type} AS {name}' for name, sql_type in types.items()) + ' WHERE false')
    columns = []
    for name in names:
        if name == 'd':
            columns.append("DATE '1970-01-01' + d AS day")
        elif kind == 'campaign' and name not in strings:
            columns.append(f'sum({name})::BIGINT AS {name}')
        else:
            columns.append(name)
    query = 'SELECT ' + ','.join(columns) + ' FROM partial'
    if kind == 'campaign':
        query += ' GROUP BY ALL'
    folder = output / (kind + '_daily')
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / 'part-0.parquet'
    if any(p != target for p in folder.glob('*.parquet')):
        raise ValueError(f'use a clean output directory: {folder}')
    con.sql(query).write_parquet(str(target), compression='snappy')
    con.execute('DROP VIEW partial')
