"""Differential checks against a separately implemented small-data oracle."""
import argparse
import subprocess
import sys
import tempfile
from pathlib import Path
import duckdb
from oracle import solve, ts

COLUMNS={'user_daily':['user_id','day','sessions','events','revenue_cents','top_category'],
'campaign_daily':['campaign_id','channel','day','sessions','purchasing_sessions','revenue_cents','assisted_sessions']}

def check(data):
    expected=solve(data)
    with tempfile.TemporaryDirectory() as tmp:
        output=Path(tmp)/'out';scratch=Path(tmp)/'scratch'
        subprocess.run([sys.executable,str(Path(__file__).with_name('pipeline.py')),str(data),str(output),'--scratch',str(scratch)],check=True,capture_output=True,text=True)
        con=duckdb.connect()
        actual={}
        for table,columns in COLUMNS.items():
            relation=con.read_parquet(str(output/table/'*.parquet'))
            assert set(relation.columns)==set(columns),(table,relation.columns)
            assert dict(zip(relation.columns,map(str,relation.types)))['day']=='DATE'
            actual[table]=relation.project(','.join(columns)).fetchall()
            assert set(actual[table])==set(expected[table]), (data,table,'missing',list(set(expected[table])-set(actual[table]))[:3],'extra',list(set(actual[table])-set(expected[table]))[:3])
            keys=[(r[0],r[1] if table=='user_daily' else r[2]) for r in actual[table]]
            assert len(keys)==len(set(keys))
        assert sum(r[2] for r in actual['user_daily'])==sum(r[3] for r in actual['campaign_daily'])
        assert sum(r[4] for r in actual['user_daily'])==sum(r[5] for r in actual['campaign_daily'])
        assert sum(r[6] for r in actual['campaign_daily'])>=sum(r[3] for r in actual['campaign_daily'] if r[0]!='organic')
        con.close()

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--fixtures',type=Path);parser.add_argument('--generator',type=Path);parser.add_argument('--seeds',type=int,default=10);parser.add_argument('--events',type=int,default=10000);args=parser.parse_args()
    for bad in ['2026-10-01T24:00:00Z','2026-10-01T12:00:60Z','2026-02-29T12:00:00Z','٢٥٠','2026-10-01T12:00:00+24:00']:
        assert ts(bad) is None,bad
    if args.fixtures:
        for case in sorted(args.fixtures.iterdir()):
            if case.is_dir():check(case/'input');print('oracle fixture',case.name,'PASS')
    if args.generator:
        with tempfile.TemporaryDirectory() as tmp:
            for seed in range(1,args.seeds+1):
                data=Path(tmp)/str(seed)
                subprocess.run([str(args.generator.resolve()),'--seed',str(seed),'--events',str(args.events),'--clicks',str(args.events//4),'--output',str(data)],check=True)
                check(data)
                # A distinct file layout and reversed record order must be equivalent.
                for folder in ('events','clicks','links'):
                    paths=sorted((data/folder).glob('*.csv'));header=None;lines=[]
                    for path in paths:
                        records=path.read_bytes().split(b'\n');header=records[0];lines.extend(records[1:-1]);path.unlink()
                    (data/folder/'shuffled.csv').write_bytes(header+b'\n'+b'\n'.join(reversed(lines))+b'\n')
                check(data);print('seed',seed,'PASS (original + reshuffled)',flush=True)

if __name__=='__main__':main()
