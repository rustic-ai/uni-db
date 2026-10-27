import sys, json, shutil
from pathlib import Path
sys.path.insert(0, '/tmp/uni-lance-upgrade/old-binding')
import uni_db
root=Path('/tmp/uni-lance-upgrade/legacy-db')
if root.exists(): shutil.rmtree(root)
def write(scope, query):
    tx=scope.tx()
    tx.execute(query)
    tx.commit()
db=uni_db.UniBuilder.open(str(root)).disable_fork_sweeper(True).build()
db.schema().label('Person').property('name','string').property('age','int').index('name','btree').apply()
db.schema().edge_type('KNOWS',['Person'],['Person']).property('since','int').apply()
db.schema().label('Document').property('title','string').property('text','string').vector('embedding',3).index('text','fulltext').index('embedding',{'type':'vector','algorithm':'hnsw_flat','metric':'l2'}).apply()
s=db.session()
write(s, "CREATE (a:Person {name:'Alice',age:30}), (b:Person {name:'Bob',age:40}), (a)-[:KNOWS {since:2020}]->(b)")
write(s, "CREATE (:Loose {name:'schemaless'})")
write(s, "CREATE (:Document {title:'Doc0',text:'alpha graph',embedding:[1.0,0.0,0.0]}), (:Document {title:'Doc1',text:'alpha storage',embedding:[0.5,0.5,0.0]}), (:Document {title:'Doc2',text:'beta index',embedding:[0.0,0.0,1.0]})")
db.flush()
db.indexes().rebuild('Person')
db.indexes().rebuild('Document')
f=s.fork('legacy-fork').build()
write(f, "CREATE (:Person {name:'Fork',age:50})")
f.flush()
n=f.fork('legacy-child').build()
write(n, "CREATE (:Person {name:'Child',age:60})")
n.flush()
write(s, "CREATE (:Person {name:'Later',age:70})")
db.flush()
queries={'people':'MATCH (n:Person) RETURN n.name AS name,n.age AS age ORDER BY name','edges':'MATCH (a)-[r:KNOWS]->(b) RETURN a.name AS src,b.name AS dst,r.since AS since','loose':'MATCH (n:Loose) RETURN n.name AS name'}
queries['fts']="CALL uni.fts.query('Document','text','alpha',10) YIELD node RETURN node.title AS title ORDER BY title"
queries['vector']="CALL uni.vector.query('Document','embedding',[1.0,0.0,0.0],2) YIELD node RETURN node.title AS title ORDER BY title"
expected={key:{name:[row.to_dict() for row in scope.query(q)] for name,q in queries.items() if key == 'primary' or name not in ['fts','vector']} for key,scope in [('primary',s),('fork',f),('child',n)]}
Path('/tmp/uni-lance-upgrade/expected.json').write_text(json.dumps(expected,indent=2)+'\n')
db.create_snapshot('legacy-snapshot')
db.tag_fork('legacy-fork','legacy-tag')
db.shutdown()
print(json.dumps(expected))
