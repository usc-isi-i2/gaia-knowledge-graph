from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import URIRef, Literal, BNode


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


