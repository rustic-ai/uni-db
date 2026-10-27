import json
import os
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, os.environ['UPGRADE_BINDING_ROOT'])
import uni_db

root = Path('/tmp/uni-lance-upgrade')
label = sys.argv[1]
results = []
for repeat in range(3):
    path = root / f'perf-{label}-{repeat}'
    db = uni_db.UniBuilder.open(str(path)).disable_fork_sweeper(True).build()
    db.schema().label('Item').property('key', 'int').property('value', 'string').index('key', 'btree').apply()
    session = db.session()
    start = time.perf_counter()
    tx = session.tx()
    tx.execute('UNWIND $items AS item CREATE (:Item {key:item.key,value:item.value})',
               {'items': [{'key': i, 'value': f'value-{i}'} for i in range(5000)]})
    tx.commit()
    db.flush()
    ingest = time.perf_counter() - start
    db.indexes().rebuild('Item')
    workloads = {
        'point': ('MATCH (n:Item) WHERE n.key = $key RETURN n.value AS value', {'key': 2500}, 1),
        'range': ('MATCH (n:Item) WHERE n.key >= 2400 AND n.key < 2600 RETURN n.key AS key', {}, 200),
        'scan': ('MATCH (n:Item) RETURN n.key AS key', {}, 5000),
    }
    timings = {'ingest_flush_seconds': ingest}
    for name, (query, params, expected) in workloads.items():
        for _ in range(3):
            assert len(list(session.query(query, params))) == expected
        start = time.perf_counter()
        for _ in range(25):
            assert len(list(session.query(query, params))) == expected
        timings[name + '_seconds_per_query'] = (time.perf_counter() - start) / 25
    db.shutdown()
    results.append(timings)
output = {'binding': os.environ['UPGRADE_BINDING_ROOT'], 'rows': 5000, 'repeats': results,
          'medians': {key: statistics.median(r[key] for r in results) for key in results[0]}}
(root / f'perf-{label}.json').write_text(json.dumps(output, indent=2) + '\n')
print(json.dumps(output))
