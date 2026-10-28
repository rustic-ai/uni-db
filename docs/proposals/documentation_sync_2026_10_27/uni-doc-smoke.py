import json, tempfile, pathlib
from uni_db import Uni, UniBuilder
checks=[]
db=UniBuilder.temporary().config({'disable_fork_sweeper':True,'compaction':{'enabled':False},'flush_stream_timeout':60,'ec_flush_threshold':10000}).build()
s=db.session()
for _ in range(2):
 with s.tx() as tx:
  tx.execute("MERGE (:Person {name:'Alice'})")
  tx.commit()
assert s.query('MATCH (n:Person) RETURN count(n) AS n').rows[0]['n']==1
checks.append('MERGE matches ordinary properties without ext_id')
s2=db.session(); t1=s.tx(); t2=s2.tx()
t1.execute("CREATE (:A {x:1})");t2.execute("CREATE (:B {x:2})")
t1.commit();t2.commit();checks.append('two live transactions execute and commit')
r=s.locy('CREATE RULE people AS MATCH (n:Person) YIELD KEY n QUERY people RETURN n')
assert len(r.derived['people'])==1 and len(r.rows())==1
checks.append('Locy Python derived property and rows()')
assert db.config()['commit_channel_capacity']==256
assert db.config()['flush_stream_timeout']==60
checks.append('configuration defaults and duration readback')
with s.tx() as tx:
 tx.execute("CREATE (:KeyA {ext_id:'unique'})");tx.commit()
try:
 with s.tx() as tx:tx.execute("CREATE (:KeyB {ext_id:'unique'})");tx.commit()
except Exception:checks.append('ext_id uniqueness spans labels')
else:raise AssertionError('cross-label ext_id accepted')
db.shutdown()
print(json.dumps(checks,indent=2))
