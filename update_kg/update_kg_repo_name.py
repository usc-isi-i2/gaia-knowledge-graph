import sys
from Updater import Updater

repo_name_src = 'gaia0531ta1'
repo_name_dst = 'gaia0531ta2'

endpoint_src = 'http://gaiadev01.isi.edu:7200/repositories/' + repo_name_src
endpoint_dst = 'http://gaiadev01.isi.edu:7200/repositories/' + repo_name_dst
output = '/Users/jenniferchen/Documents/AIDA/ta2pipline/store_data/' + repo_name_dst
graph = 'http://www.isi.edu/baseline-20190605-001'
has_jl = True
print('---')
print('your source endpoint: ', endpoint_src)
print('your destination endpoint: ', endpoint_dst)
print('your output: ', output)
print('your graph: ', graph)
print('your has jl: ', has_jl)
print('---')


up = Updater(endpoint_src, endpoint_dst, repo_name_src, output, graph, has_jl)
#up.run_delete_ori()  # run this only if creating the first named graph in the repo
up.run_load_jl(entity_clusters='clusters-baseline-20190605-001.jl')
up.run_entity_nt()
up.run_event_nt()
up.run_relation_nt()
up.run_insert_proto()
up.run_super_edge()


