"""
TODO
1. deploy mayank's data on server?
2. change normal query with SPARQL
"""
import sys
sys.path.append('/Users/eric/Git/NCC/AIDA-Interchange-Format/python/aida_interchange')
from rdflib import URIRef, Graph, BNode, Literal
from rdflib.namespace import SKOS, RDF
import aifutils
from SPARQLWrapper import SPARQLWrapper, JSON
from aida_rdf_ontologies import AIDA_ANNOTATION
import json


system = URIRef("http://isi.edu")
g = aifutils.make_graph()
endpoint = "http://localhost:3030/mayank/query"
phase_one_input = './files_from_mayank/RPI_clusters_seedling.nt'
phase_one_output = './files_from_mayank/xij_tmp.nt'
phase_two_output = './files_from_mayank/xij_output.nt'


def phase_one():
    g.load(phase_one_input, format='nt')
    clean_out_cluster_inside()
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
    g.serialize(phase_one_output, "nt")


def phase_two():
    g.load(phase_one_output, format='nt')
    # transfer all entity-entity relationships to cluster-cluster SuperEdge
    transfer_relation_to_cluster(endpoint)
    g.serialize(phase_two_output, format='nt')


def clean_out_cluster_inside():
    """
    Inside Mayank's file, he has already defined some cluster that we don't want any more
    """
    for sub in g.triples((None, RDF.type, AIDA_ANNOTATION.SameAsCluster)):
        g.remove((sub, None, None))


def make_synthetic_entity(entities):
    # Find all DatatypeProperties
    # For each, find the most common value
    datatypeproperty_copy_query = """
SELECT ?p ?o (COUNT(?o) AS ?oN)
WHERE {
  ?s ?p ?o 
  FILTER ( ?s IN ( %s ) && isLiteral(?o) )
}
GROUP BY ?p ?o"""
    # DONE {Xi}: copy all DatatypeProperty for  synthetic entity
    properties = {}
    for row in query_with_sparqlwrapper(endpoint, datatypeproperty_copy_query % (', '.join(map(URIRef.n3, map(URIRef, entities))))):
        p = row['p']['value']
        o = row['o']['value']
        on = int(row['oN']['value'])
    # for p, o, on in g.query(datatypeproperty_copy_query):
        if p in properties:
            if on <= properties[p][0]:
                continue
        properties[p] = (on, o)
    prototype = URIRef(entities[0]+'_prototype')
    aifutils.make_entity(g, prototype, system)
    for p in properties:
        g.add((prototype, URIRef(p), Literal(properties[p][1])))
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


def transfer_relation_to_cluster(endpoint):
    super_edge_count_query = """
PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
SELECT ?cluster1 ?pred ?cluster2 (COUNT(DISTINCT *) AS ?count)
WHERE {
  ?e1 a aida:Entity ;
      aida:inCluster ?cluster1 ;
      ?pred ?e2 .
  ?e2 a aida:Entity ;
      aida:inCluster ?cluster2 .
}
GROUP BY ?cluster1 ?pred ?cluster2
"""
    for row in query_with_sparqlwrapper(endpoint, super_edge_count_query):
        c1 = row['cluster1']['value']
        pred = row['pred']['value']
        c2 = row['cluster2']['value']
        count = row['count']['value']
        make_super_edge(URIRef(c1), URIRef(pred), URIRef(c2), Literal(count))


def query_with_sparqlwrapper(endpoint, query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()['results']['bindings']


# First, run phase one
# phase_one()
# input("Load the output into Fuseki, then run phase two. Ready?")
# Load output into Fuseki, then run phase two
phase_two()
