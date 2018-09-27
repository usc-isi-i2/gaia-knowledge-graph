from SPARQLWrapper import SPARQLWrapper, CSV
import json
import csv
from datetime import datetime
import random
import string

# sw = SPARQLWrapper("http://gaiadev01.isi.edu:7200/repositories/0923r0wl")
sw = SPARQLWrapper('http://localhost:3030/test/sparql')
sw.setReturnFormat(CSV)


def randstr():
    return ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(32)])


def run_entity():
    q = '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
PREFIX ldcOnt: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/SeedlingOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?e ?type ?linkTarget WHERE {
    ?e a aida:Entity .
    ?rtype rdf:subject ?e ;
           rdf:predicate rdf:type ;
           rdf:object ?type .
    OPTIONAL {
        ?e aida:link/aida:linkTarget ?linkTarget .
    }
}
'''
    print('start query ent type link', str(datetime.now()))
    sw.setQuery(q)
    rows = sw.query().convert().splitlines()[1:]
    res = list(csv.reader(rows))
    ent_json = {}
    print('start for loop', str(datetime.now()))
    dummy = 0
    for line in res:
        ent, _type, lt = line
        if not lt:
            lt = 'DUMMY:%d' % dummy
            dummy += 1
        ent_json[ent] = ['', _type, lt]

    q = '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
PREFIX ldcOnt: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/SeedlingOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?e ?name ?translate WHERE {
    ?e a aida:Entity ;
       aida:hasName ?name .
    OPTIONAL {
        ?e aida:justifiedBy [
            skos:prefLabel ?name ;
            aida:privateData [
                aida:jsonContent ?translate ;
                aida:system <http://www.rpi.edu/EDL_Translation>
            ]
        ]
    }
}
'''
    print('start query ent name translate', str(datetime.now()))
    sw.setQuery(q)
    rows = sw.query().convert().splitlines()[1:]
    res = list(csv.reader(rows))
    print('start for loop', str(datetime.now()))
    for line in res:
        e, name, translate = line
        if translate:
            name = translate
        if e not in ent_json:
            ent_json[e] = ['', '', 'DUMMY:%d' % dummy]
            dummy += 1
        ent_json[e][0] = name

    print("dummy", dummy)
    print('start dump entity', str(datetime.now()))
    with open('run1_entity.json', 'w') as f1:
        json.dump(ent_json, f1, indent=2)
    print('Done')


def run_cluster():
    qc = '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
SELECT DISTINCT ?e ?c where {
  ?x aida:cluster ?c ;
     aida:clusterMember ?e .
}
'''
    sw.setQuery(qc)
    print('start query cluster', str(datetime.now()))
    rows = sw.query().convert().splitlines()[1:]
    res = list(csv.reader(rows))
    print('start for loop cluster ', str(datetime.now()))
    cluster_json = {}
    for line in res:
        e, c = line
        if c not in cluster_json:
            cluster_json[c] = [[], []]
        cluster_json[c][0].append(e)

    qp = '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
SELECT DISTINCT ?c ?p where {
     ?c aida:prototype ?p .
     ?p aida:justifiedBy ?jp .
}
'''
    sw.setQuery(qp)
    print('start query proto', str(datetime.now()))
    rows = sw.query().convert().splitlines()[1:]
    res = list(csv.reader(rows))
    print('start for loop proto ', str(datetime.now()))
    for line in res:
        c, p = line
    if c not in cluster_json:
        cluster_json[c] = [[], []]
    cluster_json[c][1].append(p)

    print('start dump cluster', str(datetime.now()))
    with open('run1_cluster.json', 'w') as f2:
        json.dump(cluster_json, f2, indent=2)
    print('Done')


def run_evt():
    q_evt = '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?e ?type ?doc ?text ?translate ?ent
WHERE {
    ?e a aida:Event;
       aida:justifiedBy ?j .
    ?j aida:source ?doc .
    OPTIONAL {?j skos:prefLabel ?text .
      OPTIONAL {
        ?j aida:privateData [
               aida:jsonContent ?translate ;
               aida:system <http://www.rpi.edu/EDL_Translation> ] .
      }
    }
    ?r rdf:subject ?e ;
       rdf:predicate rdf:type ;
       rdf:object ?type .
    OPTIONAL {
      ?r2 rdf:subject ?e ;
          rdf:object ?ent .
      ?ent a aida:Entity .
    }
}
'''
    sw.setQuery(q_evt)
    print('start fetch evt', str(datetime.now()))
    res = sw.query().convert()['results']['bindings']
    evt_json = {}
    print('start convert evt json', str(datetime.now()))
    for line in res:
        evt_uri = line['e']['value']
        _type = line['type']['value']
        doc = line['doc']['value']
        text = ''
        if 'translate' in line:
            text = line['translate']['value']
        elif 'text' in line:
            text = line['text']['value']
        if evt_uri not in evt_json:
            evt_json[evt_uri] = {'type': _type, 'doc': doc, 'text': [], 'entities': []}
        if text:
            evt_json[evt_uri]['text'].append(text)
        if 'ent' in line:
            evt_json[evt_uri]['entities'].append(line['ent']['value'])

    print('write file', str(datetime.now()))
    with open('run1_event.json', 'w') as f:
        json.dump(evt_json, f, indent=2)
    print('Done', str(datetime.now()))


run_entity()
run_cluster()
run_evt()
