from rdflib.namespace import RDF, Namespace, XSD
from aif import aida_rdf_ontologies

AidaDomainOntologiesCommon = Namespace('https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/AidaDomainOntologiesCommon#')
# AidaSeedling = Namespace('https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/SeedlingOntology#')
# AidaInterchange = Namespace('https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#')
AidaSeedling = Namespace('https://tac.nist.gov/tracks/SM-KBP/2019/ontologies/SeedlingOntology#')
AidaInterchange = Namespace('https://tac.nist.gov/tracks/SM-KBP/2019/ontologies/InterchangeOntology#')


namespaces = {
    'aida': aida_rdf_ontologies.AIDA_ANNOTATION,
    'rdf': RDF,
    'xij': Namespace('http://isi.edu/xij-rule-set#'),
    'skos': Namespace('http://www.w3.org/2004/02/skos/core#'),
    'xsd': XSD
}


ENTITY_TYPE = [
    AidaSeedling.Facility,
    # AidaSeedling.FillerType,
    AidaSeedling.GeopoliticalEntity,
    AidaSeedling.Location,
    AidaSeedling.Organization,
    AidaSeedling.Person,
    # AidaSeedling.Age,
    # AidaSeedling.Ballot,
    # AidaSeedling.Commodity,
    # AidaSeedling.Crime,
    # AidaSeedling.Law,
    AidaSeedling.Money,
    AidaSeedling.NumericalValue,
    # AidaSeedling.Results,
    # AidaSeedling.Sentence,
    # AidaSeedling.Sides,
    AidaSeedling.Time,
    AidaSeedling.Title,
    AidaSeedling.URL,
    AidaSeedling.Weapon,
    AidaSeedling.Vehicle
]

ENTITY_TYPE_STR = [t.toPython() for t in ENTITY_TYPE]
