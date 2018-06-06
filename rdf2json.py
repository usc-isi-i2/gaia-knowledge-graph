import json
import rdflib

g = rdflib.Graph()
g.parse("sample1.turtle", format="ttl")

statements = g.query(
        """SELECT DISTINCT ?sub ?pred ?obj
        WHERE {
            { ?s rdf:type rdf:Statement . ?s rdf:predicate ?pred } UNION
            { ?s rdf:type ?pred FILTER (?pred != rdf:Statement) }
            ?s rdf:subject ?sub .
            ?s rdf:object ?obj .
       }""")

# statements = g.query(
#         """SELECT DISTINCT ?sub ?pred ?obj
#         WHERE {
#             { ?s rdf:type rdf:Statement . ?s rdf:predicate ?pred } UNION
#             { ?s rdf:type ?pred . OPTIONAL { ?s rdf:predicate ?p } . FILTER (!bound(?p)) }
#             ?s rdf:subject ?sub .
#             ?s rdf:object ?obj .
#        }""")

d = dict()
for s, p, o in statements:
    d[s] = d.get(s, dict())
    d[s][p] = d[s].get(p, list())
    d[s][p].append(o)

res = list()
for key in d:
    d[key]["uri"] = key
    res.append(d[key])

with open("output.json", "w") as f:
    f.write(json.dumps(res, indent=2))
