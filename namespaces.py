from rdflib.namespace import RDF, Namespace
from aif import aida_rdf_ontologies

namespaces = {
    'aida': aida_rdf_ontologies.AIDA_ANNOTATION,
    'rdf': RDF,
    'xij': Namespace('http://isi.edu/xij-rule-set#')
}

