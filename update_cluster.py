"""
1. upload original .nt file to fuseki
2. SPARQL-update-delete:
    ?cluster a io:SameAsCluster .
    ?cluster io:prototype ?prototype .
    ?prototype a io:Event/io:Entity .
    ?membership a io:clusterMembership;
                io:cluster ?cluster;
                io:clusterMember ?entity/?event .
3. upload phase1.nt (thus the cluster related triples were replaced)
4. SPARQL-update-insert:
    ?prototype a [max count rdf:type(exclude Entity/Event)] .
    ?prototype io:hasName [max count io:hasName] .
    ?se a xij:SuperEdge ;
        rdf:subject ?sub_cluster ;
        rdf:predicate ?predicate ;
        rdf:object ?obj_cluster ;
        cij:edgeCount ?edge_count .
"""
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from namespaces import namespaces
import json

endpoint = 'http://gaiadev01.isi.edu:3030/gaiaold/update'
sparql = SPARQLStore(endpoint)
# usage: sparql.query(query, namespaces, {'cluster': URIRef(uri)})
# (query: query string, namespaces: prefix mapping, variables mapping)

with open('all_cluster_uri.json') as f:
    all_cluster_uri = json.load(f)


def delete():
    query = """
    DELETE {
        ?cluster a aida:SameAsCluster .
        ?cluster aida:prototype ?prototype .
    }
    WHERE {
        ?cluster a aida:SameAsCluster .
        ?cluster aida:prototype ?prototype .
    }
    """
    sparql.query(query, namespaces)
