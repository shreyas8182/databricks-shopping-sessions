"""Measure start-to-exit runtime and check output invariants; no contest-score claim."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import duckdb

def invariants(output):
    con=duckdb.connect()
    for name in ('user_daily','campaign_daily'):
        con.read_parquet(str(Path(output)/name/'*.parquet')).create_view(name)
    user=con.execute('SELECT sum(sessions),sum(events),sum(revenue_cents),count(*),count(DISTINCT (user_id,day)) FROM user_daily').fetchone()
    campaign=con.execute("SELECT sum(sessions),sum(revenue_cents),sum(assisted_sessions),sum(CASE WHEN campaign_id<>'organic' THEN sessions ELSE 0 END),count(*),count(DISTINCT (campaign_id,day)) FROM campaign_daily").fetchone()
    assert user[0]==campaign[0] and user[2]==campaign[1],(user,campaign)
    assert campaign[2]>=campaign[3],campaign
    assert user[3]==user[4] and campaign[4]==campaign[5],(user,campaign)
    con.close()
    return {'sessions':user[0],'surviving_events':user[1],'revenue_cents':user[2],'user_rows':user[3],'campaign_rows':campaign[4],'assisted_sessions':campaign[2],'invariants':'PASS'}

def main():
    p=argparse.ArgumentParser();p.add_argument('data');p.add_argument('output');p.add_argument('--scratch',default='scratch');p.add_argument('--runs',type=int,default=1);p.add_argument('--report',default='benchmark-result.json');a=p.parse_args()
    results=[]
    for number in range(1,a.runs+1):
        code="import pipeline,resource,sys,json; pipeline.run(*sys.argv[1:]); r=resource.getrusage(resource.RUSAGE_SELF); print(json.dumps({'peak_rss_bytes':r.ru_maxrss*(1 if sys.platform=='darwin' else 1024)}))"
        start=time.perf_counter();child=subprocess.run([sys.executable,'-c',code,a.data,a.output,a.scratch],capture_output=True,text=True)
        print(child.stderr,file=sys.stderr,end='')
        result={'run':number,'exit_code':child.returncode,'wall_seconds':round(time.perf_counter()-start,3),'memory_limit':os.environ.get('MEMORY_LIMIT','16GB'),'threads':int(os.environ.get('NCPU','8'))}
        if child.returncode==0:
            result.update(json.loads(child.stdout.splitlines()[-1]));result.update(invariants(a.output))
        else:result['failure']=child.stderr[-2000:]
        results.append(result);Path(a.report).write_text(json.dumps({'platform':sys.platform,'data':a.data,'results':results},indent=2)+'\n');print(json.dumps(result),flush=True)
        if child.returncode:raise SystemExit(child.returncode)

if __name__=='__main__':main()
