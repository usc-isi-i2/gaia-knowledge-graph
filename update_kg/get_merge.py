from .MergedUpdater import MergedUpdater
import sys

if len(sys.argv) > 2:
    endpoint = sys.argv[1]
    output = sys.argv[2]
    MergedUpdater(endpoint, output).get_cluster()

else:
    MergedUpdater("http://localhost:3030/test", "./test_output/").run()
