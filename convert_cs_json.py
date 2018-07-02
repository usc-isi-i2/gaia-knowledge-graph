import json
from rdflib import Graph, URIRef
from rdflib.namespace import Namespace, RDF, RDFS, SKOS


AIDA = Namespace('http://www.isi.edu/aida/interchangeOntology#')

g = Graph().parse("eng.nt", format="nt")

entities = dict()
reference = dict()
# Add all aida:Event and aida:Entity
for type_ in (AIDA.Event, AIDA.Entity):
    for e in g.subjects(RDF.type, type_):
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
    for d in (entities, reference):
        d[s] = d.get(s, dict())
        d[s][p] = d[s].get(p, list())
    reference[s][p].append(o)
    if o in reference:
        entities[s][p].append(reference[o])
    else:
        entities[s][p].append(o)

for key in entities:
    entities[key]["uri"] = key
    reference[key]["uri"] = key

with open("eng.json", "w") as f:
    f.write(json.dumps(list(entities.values()), indent=2))

# output = Graph()
# for s, p, o in statements:
#     output.add((s, p, o))
# for type_ in (AIDA.Event, AIDA.Entity):
#     for e in g.subjects(RDF.type, type_):
#         output.add((e, RDF.type, type_))
#         for label in g.preferredLabel(e):
#             output.add((e, *label))
# output.serialize('eng-ld.json', format='json-ld')
