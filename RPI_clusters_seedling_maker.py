"""
TODO
1. deploy mayank's data on server?
2. change normal query with SPARQL
"""
import sys
sys.path.append('/Users/eric/Git/NCC/AIDA-Interchange-Format/python/aida_interchange')
from rdflib import URIRef, Graph, BNode
from rdflib.namespace import Namespace, SKOS, RDF
import aifutils
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from aida_rdf_ontologies import AIDA_ANNOTATION
import json
from collections import Counter
import types


super_edge_count_query = """
PREFIX aida: <http://www.isi.edu/aida/interchangeOntology#>
SELECT ?cluster1 ?pred ?cluster2 (COUNT(DISTINCT *) AS ?count)
WHERE {
  ?membership1 a aida:ClusterMembership ;
               aida:cluster ?cluster1 ;
               aida:clusterMember ?e1 .
  ?membership2 a aida:ClusterMembership ;
               aida:cluster ?cluster2 ;
               aida:clusterMember ?e2 .
  ?e1 a aida:Entity .
  ?e2 a aida:Entity .
  ?e1 ?pred ?e2 
}
GROUP
"""
update_query = """
INSERT DATA {
  %s .
}
"""
system = URIRef("http://isi.edu")
endpoint = "http://localhost:3030/mayank/query"
update_endpoint = "http://localhost:3030/mayank/update"


def add(self, xxx_todo_changeme):
    Graph.add(self, xxx_todo_changeme)
    sparql = SPARQLWrapper(update_endpoint)
    query = update_query % ' '.join(term.n3() for term in xxx_todo_changeme)
    print(query)
    sparql.setRequestMethod(POST)
    sparql.setQuery(query)
    sparql.setReturnFormat('json')
    return sparql.query()


g = aifutils.make_graph()
g.load('./files_from_mayank/RPI_clusters_seedling.nt', format='nt')
g.add = types.MethodType(add, g)


def make_synthetic_entity(entities):
    labels = [label[1] for entity in map(URIRef, entities) for label in g.preferredLabel(entity)]
    common_label = Counter(labels).most_common(1)[0][0]
    prototype = URIRef(entities[0]+'_prototype')
    aifutils.make_entity(g, prototype, system)
    g.add((prototype, SKOS.prefLabel, common_label))
    return prototype


def make_cluster(entities):
    cluster_uri = URIRef(entities[0] + '_cluster')
    prototype_uri = make_synthetic_entity(entities)
    aifutils.make_cluster_with_prototype(g, cluster_uri, prototype_uri, system)
    for entity in map(URIRef, entities):
        aifutils.mark_as_possible_cluster_member(g, entity, cluster_uri, 1.0, system)
    return cluster_uri


def make_super_edge(subject, predicate, object_, count):
    super_edge = BNode()
    g.add((super_edge, RDF.type, AIDA_ANNOTATION.SuperEdge))
    g.add((super_edge, RDF.subject, subject))
    g.add((super_edge, RDF.predicate, predicate))
    g.add((super_edge, RDF.object, object_))
    g.add((super_edge, AIDA_ANNOTATION.edgeCount, count))
    aifutils.make_system_with_uri(g, system)
    return super_edge


def transfer_relation_to_cluster():
    for row in query_with_sparqlwrapper(endpoint, super_edge_count_query):
        c1 = row['cluster1']['value']
        pred = row['pred']['value']
        c2 = row['cluster2']['value']
        count = row['count']['value']
        make_super_edge(c1, pred, c2, count)


def query_with_sparqlwrapper(endpoint, query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()['results']['bindings']



with open('./files_from_mayank/RPI_clusters_seedling_same_link_clusters.jl') as f:
    for ln, line in enumerate(f.readlines()):
        try:
            entity_list = json.loads(line)['entities']
        except json.JSONDecodeError:
            raise json.JSONDecodeError('Cannot decode line %d: %s'.format(ln, line))
        except KeyError:
            raise KeyError('Cannot find key "entities" at line %d: %s'.format(ln, line))
        # generate a synthetic entity for the most common prefLabel
        make_cluster(entity_list)
# transfer all entity-entity relationships to cluster-cluster SuperEdge
transfer_relation_to_cluster()

with open('./file_from_mayank/xij_output.ttl', 'w') as of:
    g.serialize(of, "ttl")