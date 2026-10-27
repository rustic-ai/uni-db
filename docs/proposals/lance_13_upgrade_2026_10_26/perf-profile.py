import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.environ['UPGRADE_BINDING_ROOT'])
import uni_db

label = sys.argv[1]
db = uni_db.UniBuilder.open('/tmp/uni-lance-upgrade/perf-' + label + '-0').disable_fork_sweeper(True).build()
s = db.session()
out = {}
for name, query, params, expected in [
    ('point', 'MATCH (n:Item) WHERE n.key = $key RETURN n.value AS value', {'key': 2500}, 1),
    ('range', 'MATCH (n:Item) WHERE n.key >= 2400 AND n.key < 2600 RETURN n.key AS key', {}, 200),
]:
    explain = s.query_with(query).params(params).explain()
    rows, profile = s.query_with(query).params(params).profile()
    assert len(rows) == expected
    assert any(op.get('index_hits', 0) > 0 for op in profile.operators), profile.operators
    out[name] = {'index_usage': explain.index_usage, 'operators': profile.operators, 'rows': len(rows)}
db.shutdown()
Path('/tmp/uni-lance-upgrade/perf-profile-' + label + '.json').write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps(out))
