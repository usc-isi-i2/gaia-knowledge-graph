def get_q():
    return '''PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?e ?name ?translate ?type ?linkTarget ?txt_grained_type
WHERE {
    ?e a aida:Entity .
    OPTIONAL {
       ?e aida:hasName ?name .
    }
    OPTIONAL {
       ?e aida:link/aida:linkTarget ?linkTarget .
    }
    OPTIONAL {
        ?e aida:link/aida:privateData [
             aida:jsonContent ?txt_grained_type ;
             aida:system <http://www.rpi.edu/EDL_FineGrained>
        ]
    }
    ?r a rdf:Statement ;
        rdf:subject ?e ;
        rdf:predicate rdf:type ;
        rdf:object ?type .
    OPTIONAL {
        ?e aida:justifiedBy ?j .
        ?j skos:prefLabel ?name ;
            aida:privateData ?pd .
        ?pd aida:jsonContent ?translate ;
            aida:system <http://www.rpi.edu/EDL_Translation>
    }
}
'''
