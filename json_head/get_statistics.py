
# from SPARQLWrapper import SPARQLWrapper
# sw = SPARQLWrapper('http://gaiadev01.isi.edu:7200/repositories/1003r2nl')
# sw.setQuery('''select * where {?s ?p ?o} limit 1''')
# sw.setReturnFormat('json')
# x = sw.query().convert()
# print(x)
# exit()

import json
from json_head.Selector import Selector


def update_cur(n, t, l, cur):
    if t not in cur:
        cur[t] = {'others': {}}
    if l.split(':', 1)[-1].startswith('m.'):
        if l not in cur[t]:
            cur[t][l] = {}
    else:
        l = 'others'
    if not n:
        n = 'NO_NAME'
    if n not in cur[t][l]:
        cur[t][l][n] = 0
    cur[t][l][n] += 1

def get_ta1_clusters(ep):
    se = Selector(ep)
    rows = se.run('''
    select distinct ?c ?e ?name ?translate ?type ?linkTarget where {
        ?mem aida:cluster ?c ;
             aida:clusterMember ?e .
        ?e a aida:Entity .
        ?rtype rdf:subject ?e ;
               rdf:predicate rdf:type ;
               rdf:object ?type .
        OPTIONAL {
            ?e aida:link/aida:linkTarget ?linkTarget .
        }
        OPTIONAL {
            ?e aida:hasName ?name .
            OPTIONAL {
                ?e aida:justifiedBy [
                    skos:prefLabel ?name ;
                    aida:privateData [
                        aida:jsonContent ?translate ;
                        aida:system <http://www.rpi.edu/EDL_Translation>
                    ]
                ]
            }
        }
    }
    ''')

    res = {}
    for row in rows:
        c, e, name, trans, _type, link = row
        if c not in res:
            res[c] = {}
        res[c][e] = [trans or name, _type.rsplit('#', 1)[-1], link or '']

    rank = []
    for c, ents in res.items():
        cur = {}
        for e, attr in ents.items():
            n, t, l = attr
            update_cur(n, t, l, cur)
        rank.append({
            'cluster_uri': c,
            'cnt': len(ents),
            'entities': cur
        })
    rank.sort(key=lambda x: x['cnt'], reverse=True)
    with open('ta1_cluster.json', 'w') as f:
        json.dump(rank, f, indent=2, ensure_ascii=False)


def get_ta2_rank(ta2_res):
    rank = []
    with open(ta2_res) as f:
        for l in f.readlines():
            entities = json.loads(l).get('entities')
            cur = {}
            for n, t, l in entities:
                update_cur(n, t, l, cur)
            rank.append({
                'cnt': len(entities),
                'entities': cur
            })
        print(len(rank))
        rank.sort(key=lambda x: x['cnt'], reverse=True)
        with open('ta2_cluster.json', 'w') as f:
            json.dump(rank, f, indent=2, ensure_ascii=False)


def get_proto(ep):
    se = Selector(ep)
    rows = se.run('''
    select distinct ?c ?p where {
        ?c aida:prototype ?p .
        FILTER NOT EXISTS {
            ?mem aida:cluster ?c ;
                aida:clusterMember ?p .
        }
    }
    ''')


    import json
    with open('ta1_prototype.json', 'w') as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


get_ta1_clusters('http://localhost:7200/repositories/1003r2nl')
# get_ta2_rank('/Users/dongyuli/isi/jl_1003r1nl_2/entity_with_attr.jl')
