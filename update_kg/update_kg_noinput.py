import sys
from Updater import Updater

endpoint = 'http://localhost:3030/run1_clean2'
output = 'nas/home/dongyul/run1jl_new'
graph = ''
has_jl = 'True'
print('---')
print('your endpoint: ', endpoint)
print('your output: ', output)
print('your graph: ', graph)
print('your has jl: ', has_jl)
print('---')

steps = ['run_load_jl', 'run_delete_ori', 'run_entity_nt', 'run_event_nt', 'run_relation_nt', 'run_insert_proto', 'run_super_edge']

print('\n'.join(['Step %d : %s ' % (i, steps[i]) for i in range(len(steps))]))

start, end = '0', '6'
print('your start end: ', start, end)

up = Updater(endpoint, output, graph, True if has_jl == 'True' else False)
runs = [up.run_load_jl, up.run_delete_ori, up.run_entity_nt, up.run_event_nt, up.run_relation_nt, up.run_insert_proto, up.run_super_edge]

for i in range(int(start), int(end)+1):
    runs[i]()


