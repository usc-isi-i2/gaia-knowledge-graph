from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import URIRef, Literal, BNode
from rdflib.namespace import RDF
import sys
sys.path.append('/Users/eric/Git/NCC/AIDA-Interchange-Format/python/aida_interchange')
import aifutils
from aida_rdf_ontologies import AIDA_ANNOTATION


def ask_with_wrapper(endpoint, ask_query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(ask_query)
    sparql.setReturnFormat(JSON)
    result = sparql.query().convert()
    return result['boolean']



def query_with_wrapper(endpoint, query):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    result = sparql.query().convert()
    vars = result['head']['vars']
    bindings = result['results']['bindings']
    results = [tuple(resolve(row.get(var, None)) for var in vars) for row in bindings]
    return results


def resolve(binding):
    if not binding:
        return None
    if binding['type'] == 'uri':
        return URIRef(binding['value'])
    elif binding['type'] == 'literal':
        return Literal(binding['value'], lang=binding.get('lang'), datatype=binding.get('datatype'))
    elif binding['type'] == 'bnode':
        return BNode(value=binding['value'])
    elif binding['type'] == 'typed-literal':
        print("Don't suppport Typed-literal")


def make_super_edge(g, subject, predicate, object_, count, system):
    super_edge = BNode()
    g.add((super_edge, RDF.type, AIDA_ANNOTATION.SuperEdge))
    g.add((super_edge, RDF.subject, subject))
    g.add((super_edge, RDF.predicate, predicate))
    g.add((super_edge, RDF.object, object_))
    g.add((super_edge, AIDA_ANNOTATION.edgeCount, count))
    aifutils.make_system_with_uri(g, system)
    return super_edge


def make_cluster(g, cluster_uri, prototype_uri, entities, system):
    aifutils.make_cluster_with_prototype(g, cluster_uri, prototype_uri, system)
    for entity in map(URIRef, entities):
        aifutils.mark_as_possible_cluster_member(g, entity, cluster_uri, 1.0, system)
    return cluster_uri


def text_justify(words, max_width):
    words = words.split()
    res, cur, num_of_letters = [], [], 0
    max_ = 0
    for w in words:
        if num_of_letters + len(w) + len(cur) > max_width:
            res.append(' '.join(cur))
            max_ = max(max_, num_of_letters)
            cur, num_of_letters = [], 0
        cur.append(w)
        num_of_letters += len(w)
    return res + [' '.join(cur).center(max_)]