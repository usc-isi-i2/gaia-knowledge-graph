from .MergedUpdater import MergedUpdater
import sys
import json

if len(sys.argv) > 2:
    endpoint = sys.argv[1]
    output = sys.argv[2]
    mu = MergedUpdater(endpoint, output)
    mu.get_json_head_entity()
    cluster = mu.get_cluster()
    with open(mu.outputs_prefix + 'entity.json', 'w') as f:
        json.dump(mu.entity_json, f, indent=2)
    with open(mu.outputs_prefix + 'cluster.json', 'w') as f:
        json.dump(cluster, f, indent=2)
else:
    MergedUpdater("http://localhost:3030/test", "./test_output/").run()
