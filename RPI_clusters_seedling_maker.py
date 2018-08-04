"""
TODO
1. deploy mayank's data on server?
2. change normal query with SPARQL
"""
from rdflib import URIRef, Graph, BNode, Literal
from rdflib.namespace import SKOS, RDF
import json
import sys

sys.path.append('/Users/eric/Git/NCC/AIDA-Interchange-Format/python/aida_interchange')
import aifutils
from aida_rdf_ontologies import AIDA_ANNOTATION
from utils import make_cluster, make_super_edge, query_with_wrapper

system = URIRef("http://isi.edu")
# endpoint = "http://localhost:3030/mayank/query"
endpoint = 'http://kg2018a.isi.edu:3030/mayank/sparql'
phase_one_input = './files_from_mayank/RPI_clusters_seedling_without_cluster.nt'
cluster_jsonl = './files_from_mayank/RPI_seedling_entity_event_clusters.jl'
phase_one_output = './files_from_mayank/xij_tmp.nt'
phase_two_output = './files_from_mayank/xij_output.nt'


def phase_one():
    g = aifutils.make_graph()
    g.load(phase_one_input, format='nt')
    # clean_out_cluster_inside(g)
    # g.serialize(phase_one_tmp, format='nt')
    with open(cluster_jsonl) as f:
        for ln, line in enumerate(f.readlines()):
            try:
                json_object = json.loads(line)
                if 'entities' in json_object:
                    member_list = json_object['entities']
                else:
                    member_list = [json_object['events']]
            except json.JSONDecodeError:
                raise json.JSONDecodeError('Cannot decode line %d: %s'.format(ln, line))
            except KeyError:
                raise KeyError('Cannot find key "entities" at line %d: %s'.format(ln, line))
            # generate a synthetic entity for the most common prefLabel
            prototype = make_synthetic_entity(g, member_list)
            cluster_uri = URIRef(member_list[0]+'-cluster')
            make_cluster(g, cluster_uri, prototype, member_list, system)
    g.serialize(phase_one_output, "nt")


def phase_two():
    g = aifutils.make_graph()
    g.load(phase_one_output, format='nt')
    # transfer all entity-entity relationships to cluster-cluster SuperEdge
    transfer_relation_to_cluster(g, endpoint)
    g.serialize(phase_two_output, format='nt')


def clean_out_cluster_inside(g):
    """
    Inside Mayank's file, he has already defined some cluster that we don't want any more
    """
    for sub, _, _ in g.triples((None, RDF.type, AIDA_ANNOTATION.SameAsCluster)):
        g.remove((sub, None, None))


def make_synthetic_entity(g, entities):
    prototype = URIRef(entities[0]+'-prototype')
    entities_query_set = ', '.join(map(URIRef.n3, map(URIRef, entities)))
    # DONE {xij} add top two most common type for it
    type_copy_query = """
SELECT ?type
WHERE {
  ?s a ?type
  FILTER ( ?s IN ( %s ) )
}
GROUP BY ?type
ORDER BY DESC(COUNT(?type))
LIMIT 2 """ % entities_query_set
    for type_, in query_with_wrapper(endpoint, type_copy_query):
        g.add((prototype, RDF.type, type_))
    # Find all DatatypeProperties
    # For each, find the most common value
    datatypeproperty_copy_query = """
SELECT ?p ?o (COUNT(?o) AS ?oN)
WHERE {
  ?s ?p ?o
  FILTER ( ?s IN ( %s ) && isLiteral(?o) )
}
GROUP BY ?p ?o""" % entities_query_set
    # DONE {Xi}: copy all DatatypeProperty for a synthetic entity
    properties = {}
    for p, o, on in query_with_wrapper(endpoint, datatypeproperty_copy_query):
        on = int(on)
        if p in properties:
            if on <= properties[p][0]:
                continue
        properties[p] = (on, o)

    aifutils.make_entity(g, prototype, system)
    for p in properties:
        g.add((prototype, URIRef(p), Literal(properties[p][1])))
    return prototype


def transfer_relation_to_cluster(g, endpoint):
    super_edge_count_query = """
PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
PREFIX xij: <http://isi.edu/xij-rule-set#>
SELECT ?cluster1 ?p ?cluster2 (COUNT(*) AS ?edgeN)
WHERE {
  ?e1 xij:inCluster ?cluster1 ;
      ?p ?e2 .
  ?e2 xij:inCluster ?cluster2 .
}
GROUP BY ?cluster1 ?p ?cluster2 
"""
    for c1, pred, c2, count in query_with_wrapper(endpoint, super_edge_count_query):
        make_super_edge(g, c1, pred, c2, count, system)


# First, run phase one
phase_one()
input("Load the output into Fuseki, then run phase two. Ready?")
# Load output into Fuseki, then run phase two
print("Running phase two...")
phase_two()
