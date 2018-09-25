from SPARQLWrapper import SPARQLWrapper
from namespaces import namespaces

PREFIX = ''.join(['PREFIX %s: <%s>\n' % (abbr, full) for abbr, full in namespaces.items()])


class Counter(object):
    def __init__(self, endpoint):
        self.sw = SPARQLWrapper(endpoint + '/query')

    def select_query(self, q):
        self.sw.setQuery(PREFIX + q)
        self.sw.setReturnFormat('json')
        bindings = self.sw.query().convert()['results']['bindings']
        return bindings

    def get_cnts(self):
        res = {}
        for type_ in ('Entity', 'Event', 'Relation', 'SameAsCluster', 'ClusterMembership'):
            q = '''
            select (count(?x) as ?cnt)
            where {
              ?x a aida:%s
            }
            ''' % type_
            cnt = int(self.select_query(q)[0]['cnt']['value'])
            res[type_] = cnt
        return res

    def get_clusters(self):
        q = '''
        select ?c ?name (count(?e) as ?cnt) where {
          ?x aida:cluster ?c ;
             aida:clusterMember ?e .
          ?c aida:prototype ?p .
          ?p aida:hasName ?name .
        }groupby ?c ?name orderby desc(?cnt)
        '''
        res = {}
        for x in self.select_query(q):
            res[x['name']['value']] = [x['cnt']['value'], x['c']['value']]
        import json
        with open('entity_cluster.json', 'w') as f:
            json.dump(res, f, indent=2)


print(Counter('http://gaiadev01.isi.edu:3030/0923r1aug').get_cnts())
