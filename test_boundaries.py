import json
from pathlib import Path
import tempfile
import unittest
from validate import check
from oracle import ts

class BoundaryTests(unittest.TestCase):
    def case(self,events='',clicks='',links=''):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);p=Path(tmp.name)
        for folder,header,body in [('events','event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents',events),('clicks','click_id,user_id,campaign_id,ts',clicks),('links','user_id_a,user_id_b',links)]:
            (p/folder).mkdir();(p/folder/'part.csv').write_bytes((header+'\n'+body).encode())
        (p/'products.csv').write_text('product_id,category\np1,books\n');(p/'campaigns.csv').write_text('campaign_id,channel\nc1,search\nc2,social\n')
        (p/'manifest.json').write_text(json.dumps({'window_start':'2026-10-01T00:00:00Z','window_end':'2026-10-15T00:00:00Z'}));return p
    def test_equal_time_campaign_assists(self):
        check(self.case('e,u,2026-10-03T10:00:00Z,2026-10-03T10:00:00Z,purchase,p1,10\n','a,u,c1,2026-10-03T09:00:00Z\nb,u,c2,2026-10-03T09:00:00Z\n'))
    def test_invalid_clock_values(self):
        check(self.case('a,u,2026-10-03T24:00:00Z,2026-10-04T00:00:00Z,view,p1,\nb,u,2026-10-03T12:00:60Z,2026-10-03T12:01:00Z,view,p1,\n'))
    def test_equal_timestamp_peers(self):
        early=ts('2026-10-03T10:00:00Z');later=early+1800001
        event_rows=[f'a,u,{early},2026-10-03T10:00:00Z,view,p1,',f'b,u,{later},2026-10-03T10:30:01Z,view,p1,',f'c,u,{later},2026-10-03T10:30:01Z,purchase,p1,10',f'd,u,{later},2026-10-03T10:30:01Z,view,p1,']
        for ordered in (event_rows,list(reversed(event_rows))):
            check(self.case('\n'.join(ordered)+'\n'))
    def test_empty_outputs(self):check(self.case())
    def test_embedded_cr_identity(self):
        check(self.case('e,u\rx,2026-10-03T10:00:00Z,2026-10-03T10:00:00Z,view,p1,\n',links='a,u\rx\n'))
    def test_leading_zero_timestamp_and_price(self):
        check(self.case('e,u,0000000000000000000000000001791021600000,2026-10-03T10:00:00Z,purchase,p1,00000000000000000000000000000000010\n'))
    def test_malformed_earliest_duplicate(self):
        check(self.case('e,u,bad,2026-10-03T10:00:00Z,view,p1,\ne,u,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,\n'))

if __name__=='__main__':unittest.main()
