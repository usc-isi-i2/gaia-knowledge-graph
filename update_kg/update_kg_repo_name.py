import sys
from Updater import Updater

repo_name = 'validation-test'

endpoint = 'http://gaiadev01.isi.edu:7200/repositories/' + repo_name
output = '/Users/jenniferchen/Documents/AIDA/ta2pipline/store_data/' + repo_name
graph = 'http://www.isi.edu/baseline'
has_jl = True
print('---')
print('your endpoint: ', endpoint)
print('your output: ', output)
print('your graph: ', graph)
print('your has jl: ', has_jl)
print('---')


up = Updater(endpoint, repo_name, output, graph, has_jl)
runs = [up.run_delete_ori,
        up.run_load_jl,
        up.run_entity_nt,
        up.run_event_nt,
        up.run_relation_nt,
        up.run_insert_proto,
        up.run_super_edge]

for run in runs:
    run()


