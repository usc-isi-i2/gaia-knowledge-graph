import sys
from Updater import Updater

repo_name = sys.argv[1]

endpoint = 'http://localhost:7200/repositories/' + repo_name
output = '/nas/home/dongyul/jl_' + repo_name + '/'
graph = ''
has_jl = 'True'
print('---')
print('your endpoint: ', endpoint)
print('your output: ', output)
print('your graph: ', graph)
print('your has jl: ', has_jl)
print('---')

steps = ['run_delete_ori', 'run_load_jl', 'run_entity_nt', 'run_event_nt', 'run_relation_nt', 'run_insert_proto', 'run_super_edge']

print('\n'.join(['Step %d : %s ' % (i, steps[i]) for i in range(len(steps))]))

input('PLEASE MAKE SURE!!!!!!!!!!!!!!')

up = Updater(endpoint, output, graph, True if has_jl == 'True' else False)
runs = [up.run_delete_ori, up.run_load_jl, up.run_entity_nt, up.run_event_nt, up.run_relation_nt, up.run_insert_proto, up.run_super_edge]

for run in runs:
    run()


