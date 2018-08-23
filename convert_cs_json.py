import json
import re
from rdflib import Graph, URIRef
from rdflib.namespace import Namespace, RDF, RDFS, SKOS


AIDA = Namespace('http://www.isi.edu/aida/interchangeOntology#')
URI_PATTERN = re.compile(r'((?:^http:|^urn:|^info:|^ftp:|^https:).*/(?:.*#)?)(.*)')


def name_process(uri):
    m = URI_PATTERN.match(uri)
    if m:
        prefix, name = m.groups()
        name = name.replace('.', '_').replace(':', '_')
        return prefix + name
    return uri


g = Graph().parse("eng.nt", format="nt")

entities = dict()
reference = dict()
# Add all aida:Event and aida:Entity
for type_ in (AIDA.Event, AIDA.Entity):
    for e in g.subjects(RDF.type, type_):
        e = name_process(e)
        for d in (entities, reference):
            d[e] = d.get(e, dict())
            d[e][RDF.type] = d[e].get(RDF.type, list())
            d[e][RDF.type].append(type_)
            for pred, label in g.preferredLabel(e):
                d[e][pred] = d[e].get(pred, list())
                d[e][pred].append(label)

# Add all sub pred obj
statements = g.query(
        """SELECT DISTINCT ?sub ?pred ?obj
        WHERE {
            { ?s rdf:type rdf:Statement . ?s rdf:predicate ?pred } UNION
            { ?s rdf:type ?pred FILTER (?pred != rdf:Statement) }
            ?s rdf:subject ?sub .
            ?s rdf:object ?obj .
       }""")
for s, p, o in statements:
    s = name_process(s)
    p = name_process(p)
    o = name_process(o)
    for d in (entities, reference):
        d[s] = d.get(s, dict())
        d[s][p] = d[s].get(p, list())
    reference[s][p].append(o)
    if o in reference:
        entities[s][p].append(reference[o])
    else:
        entities[s][p].append(o)

for key in entities:
    entities[key]["@id"] = key
    reference[key]["@id"] = key

with open("eng.json", "w") as f:
    f.write(json.dumps(list(entities.values()), indent=2))
