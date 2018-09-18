q = '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
PREFIX ldcOnt: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/SeedlingOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX schema: <http://schema.org/>
INSERT {
  GRAPH <http://www.isi.com/supergraph> {
    ?se a rdf:Statement ;
        rdf:subject ?s ;
        rdf:predicate ?p ;
        rdf:object ?o ;
  		aida:count ?count .
    ?s aida:justifiedBy ?js .
    ?js ?jsp ?jso .
    ?jso ?jsop ?jsoo .
    ?o aida:justifiedBy ?jo .
    ?jo ?jop ?joo .
    ?joo ?joop ?jooo .
    ?stype a rdf:Statement ;
           rdf:subject ?s ;
           rdf:predicate rdf:type ;
           rdf:object ?type_of_s .
    ?otype a rdf:Statement ;
           rdf:subject ?o ;
           rdf:predicate rdf:type ;
           rdf:object ?type_of_o .
    ?s aida:hasName ?names .
    ?o aida:hasName ?nameo .
  }
}
WHERE {
    ?se a rdf:Statement ;
        rdf:subject ?s ;
        rdf:predicate ?p ;
        rdf:object ?o ;
  		aida:count ?count .
  ?s aida:justifiedBy ?js .
  ?js ?jsp ?jso .
  OPTIONAL { ?jso ?jsop ?jsoo}
  ?o aida:justifiedBy ?jo .
  ?jo ?jop ?joo .
  OPTIONAL { ?joo ?joop ?jooo}
  ?stype a rdf:Statement ;
         rdf:subject ?s ;
         rdf:predicate rdf:type ;
         rdf:object ?type_of_s .
  ?otype a rdf:Statement ;
         rdf:subject ?o ;
         rdf:predicate rdf:type ;
         rdf:object ?type_of_o .
  OPTIONAL {?s aida:hasName ?names}
  OPTIONAL {?o aida:hasName ?nameo}
}

'''

