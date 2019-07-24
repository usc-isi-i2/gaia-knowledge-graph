import sys
from Updater import Updater

endpoint = input('Enter localhost with or without / (no route name like "update", "query")\n')
name = input('Enter a name prefix (i.e. repository name)\n')
output = input('Enter output folder with / \n')
graph = input('Enter graph name \n')
has_jl = input('Enter "True" if you has jl, otherwise press enter\n')
print('---')
print('your endpoint: ', endpoint)
print('your name prefix: ', name)
print('your output: ', output)
print('your graph: ', graph)
print('your has jl: ', has_jl)
print('---')

steps = ['run_load_jl', 'run_delete_ori', 'run_system', 'run_entity_nt', 'run_event_nt', 'run_relation_nt', 'run_insert_proto', 'run_super_edge']

print('\n'.join(['Step %d : %s ' % (i, steps[i]) for i in range(len(steps))]))

start, end = input('Enter step range you want to run like "0,2"(inclusive)\n').split(',')
print('your start end: ', start, end)

up = Updater(endpoint, name, output, graph, True if has_jl == 'True' else False)
runs = [up.run_load_jl,
        up.run_delete_ori,
        up.run_system,
        up.run_entity_nt,
        up.run_event_nt,
        up.run_relation_nt,
        up.run_insert_proto,
        up.run_super_edge]

for i in range(int(start), int(end)+1):
    runs[i]()


