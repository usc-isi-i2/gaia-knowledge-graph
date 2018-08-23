from rdflib.namespace import RDF, Namespace
from aif import aida_rdf_ontologies

namespaces = {
    'aida': aida_rdf_ontologies.AIDA_ANNOTATION,
    'rdf': RDF,
    'xij': Namespace('http://isi.edu/xij-rule-set#'),
    'skos': Namespace('http://www.w3.org/2004/02/skos/core#'),
}
