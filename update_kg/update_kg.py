import sys
from Updater import Updater

endpoint = input('Enter localhost with or without / (no route name like "update", "query")')
output = input('Enter output folder with / ')
graph = input('Enter graph name ')
has_jl = input('Enter "True" if you has jl, otherwise press enter')
print('---')
print('you endpoint: ', endpoint)
print('you output: ', output)
print('you graph: ', graph)
print('you has jl: ', has_jl)
print('---')

steps = ['run_load_jl', 'run_entity_nt', 'run_event_nt', 'run_relation_nt', 'run_insert_proto', 'run_super_edge']

print('\n'.join(['Step %d : %s ' % (i, steps[i]) for i in range(len(steps))]))

start, end = input('Enter step range you want to run like "0,2"(inclusive)').split(',')

up = Updater(endpoint, output, graph, True if has_jl == 'True' else False)
runs = [up.run_load_jl, up.run_entity_nt, up.run_event_nt, up.run_relation_nt, up.run_insert_proto, up.run_super_edge]

for i in range(int(start), int(end)+1):
    runs[i]()


