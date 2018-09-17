import sys
from Updater import Updater

if len(sys.argv) > 2:
    endpoint = sys.argv[1]
    output = sys.argv[2]
    run_from_jl = False
    if sys.argv[3] == 'se':
        Updater(endpoint, output).run_super_edge()
        exit()
    if len(sys.argv) > 3:
        run_from_jl = True
    Updater(endpoint, output).run(run_from_jl)
else:
    Updater("http://localhost:3030/test", "./test_output/").run()
