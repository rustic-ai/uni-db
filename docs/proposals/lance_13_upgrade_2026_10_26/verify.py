import os, sys, json
from pathlib import Path
sys.path.insert(0, os.environ['UPGRADE_BINDING_ROOT'])
import uni_db
root=Path('/tmp/uni-lance-upgrade')
expected=json.loads((root/'expected.json').read_text())
queries={'people':'MATCH (n:Person) RETURN n.name AS name,n.age AS age ORDER BY name','edges':'MATCH (a)-[r:KNOWS]->(b) RETURN a.name AS src,b.name AS dst,r.since AS since','loose':'MATCH (n:Loose) RETURN n.name AS name','fts':"CALL uni.fts.query('Document','text','alpha',10) YIELD node RETURN node.title AS title ORDER BY title",'vector':"CALL uni.vector.query('Document','embedding',[1.0,0.0,0.0],2) YIELD node RETURN node.title AS title ORDER BY title"}
def rows(scope,q): return [r.to_dict() for r in scope.query(q)]
def write(scope,q):
    tx=scope.tx(); tx.execute(q); tx.commit()
db=uni_db.UniBuilder.open(str(root/'legacy-db')).disable_fork_sweeper(True).build()
s=db.session(); f=s.fork('legacy-fork').build(); n=f.fork('legacy-child').build()
scopes={'primary':s,'fork':f,'child':n}
mode=sys.argv[1]
if mode=='mutate':
    for key,scope in scopes.items():
        for name,want in expected[key].items(): assert rows(scope,queries[name])==want,(key,name)
    assert 'legacy-tag' in db.list_fork_tags('legacy-fork')
    for key,scope in scopes.items():
        name={'primary':'Upgraded','fork':'ForkUpgrade','child':'ChildUpgrade'}[key]
        write(scope,f"CREATE (:Person {{name:'{name}',age:99}})")
        scope.flush()
    db.compaction().compact('Person')
    db.indexes().rebuild('Document')
    observed={key:{name:rows(scope,queries[name]) for name in expected[key]} for key,scope in scopes.items()}
    (root/'upgraded-expected.json').write_text(json.dumps(observed,indent=2)+'\n')
else:
    observed=json.loads((root/'upgraded-expected.json').read_text())
    for key,scope in scopes.items():
        for name,want in observed[key].items(): assert rows(scope,queries[name])==want,(key,name)
snapshot=next(x for x in db.list_snapshots() if x.name=='legacy-snapshot')
pinned=db.session(); pinned.pin_to_version(snapshot.snapshot_id)
assert rows(pinned,queries['people'])==expected['primary']['people']
db.shutdown()
print(json.dumps({'mode':mode,'binding':os.environ['UPGRADE_BINDING_ROOT'],'checks':'primary, forks, nested forks, scalar/vector/FTS results, snapshot and tag','observed':observed}))
