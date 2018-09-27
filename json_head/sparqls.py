PREFIX = '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
PREFIX ldcOnt: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/SeedlingOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
'''

ent_type_link = '''
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

ent_name = '''
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

cluster = '''
SELECT DISTINCT ?e ?c where {
  ?e a aida:Entity .
  ?x aida:cluster ?c ;
     aida:clusterMember ?e .
}
'''

proto = '''
SELECT DISTINCT ?c ?p where {
    ?c aida:prototype ?p  .
    ?p a aida:Entity ;
       aida:justifiedBy ?jp .
}
'''

evt = '''
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
