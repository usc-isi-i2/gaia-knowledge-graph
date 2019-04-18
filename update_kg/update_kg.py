import sys
from Updater import Updater

# endpoint = input('Enter localhost with or without / (no route name like "update", "query")\n')
endpoint = endpoint = 'http://gaiadev01.isi.edu:7200/repositories/validation-test'
# output = input('Enter output folder with / \n')
output = '/Users/jenniferchen/Desktop/validation-test/'
# graph = input('Enter graph name \n')
graph = None
# has_jl = input('Enter "True" if you has jl, otherwise press enter\n')
has_jl = True
print('---')
print('you endpoint: ', endpoint)
print('you output: ', output)
print('you graph: ', graph)
print('you has jl: ', has_jl)
print('---')

steps = ['run_load_jl', 'run_delete_ori', 'run_system', 'run_entity_nt', 'run_event_nt', 'run_relation_nt', 'run_insert_proto', 'run_super_edge']

print('\n'.join(['Step %d : %s ' % (i, steps[i]) for i in range(len(steps))]))

# start, end = input('Enter step range you want to run like "0,2"(inclusive)\n').split(',')
start = 0
end = 7
print('your start end: ', start, end)

# up = Updater(endpoint, output, graph, True if has_jl == 'True' else False)
up = Updater(endpoint, output, graph, has_jl)
runs = [up.run_load_jl, up.run_delete_ori, up.run_system, up.run_entity_nt, up.run_event_nt, up.run_relation_nt, up.run_insert_proto, up.run_super_edge]

for i in range(int(start), int(end)+1):
    runs[i]()


