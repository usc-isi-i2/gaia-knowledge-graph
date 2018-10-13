import random, string


def delete_all():
    return '''
DELETE {?s ?p ?o}
WHERE {?s ?p ?o}
    '''


def delete_ori_cluster():
    return '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

DELETE {
    ?cluster a aida:SameAsCluster ;
             aida:prototype ?prototype ;
             aida:system ?system .
}
WHERE {
    ?cluster a aida:SameAsCluster ;
             aida:prototype ?prototype ;
             aida:system ?system .
}
    '''


def delete_ori_clusterMember():
    return '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

DELETE {
    ?clusterMembership a aida:ClusterMembership ;
        aida:cluster ?cluster ;
        aida:clusterMember ?clusterMember ;
        aida:confidence ?conf ;
        aida:system ?sys .
    ?conf a aida:Confidence ;
        aida:confidenceValue ?cv ;
        aida:system ?sys_conf
}
WHERE {
    ?clusterMembership a aida:ClusterMembership ;
        aida:cluster ?cluster ;
        aida:clusterMember ?clusterMember ;
        aida:confidence ?conf ;
        aida:system ?sys .
    ?conf a aida:Confidence ;
        aida:confidenceValue ?cv ;
        aida:system ?sys_conf .
}
    '''


def get_entity():
    return '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
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


def get_event():
    return '''
PREFIX aida: <https://tac.nist.gov/tracks/SM-KBP/2018/ontologies/InterchangeOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?e ?type ?doc ?text ?translate ?ent
WHERE {
    ?e a aida:Event;
       aida:justifiedBy [
            aida:source ?doc ;
            skos:prefLabel ?text ] .
    OPTIONAL {
      ?j aida:privateData [
            aida:jsonContent ?translate ;
            aida:system <http://www.rpi.edu/EDL_Translation> ] .
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


def get_relation(graph):
    open_clause = close_clause = ''
    if graph:
        open_clause = 'GRAPH <%s> {' % graph
        close_clause = '}'
    return '''SELECT distinct ?e ?type ?p ?cluster
    WHERE {
        ?e a aida:Relation .
        ?r rdf:subject ?e ;
           rdf:predicate rdf:type ;
           rdf:object ?type .

        ?r2 rdf:subject ?e ;
           rdf:predicate ?p ;
           rdf:object ?ent .
        %s
            ?mem aida:cluster ?cluster ;
                aida:clusterMember ?ent .
        %s
    }
    ''' % (open_clause, close_clause)


def proto_name(graph):
    open_clause = close_clause = ''
    if graph:
        open_clause = 'GRAPH <%s> {' % graph
        close_clause = '}'
    return '''
insert { 
    %s 
        ?prototype aida:hasName ?name .
    %s }
where {

    select ?prototype (max(?label1) as ?name)
    where {
        {
            select ?prototype (max(?cnt) as ?max)
            where {
                select ?prototype ?label (count(?label) as ?cnt)
                where {
                    %s
                        ?c aida:prototype ?prototype .
                        ?membership aida:cluster ?c ; aida:clusterMember ?e .
                    %s
                    ?e aida:hasName ?label .
                } group by ?prototype ?label
            } group by ?prototype
        }
        {
            select ?prototype ?label1 (count(?label1) as ?max)
            where {
                    %s
                        ?c1 aida:prototype ?prototype .
                        ?membership1 aida:cluster ?c1 ; aida:clusterMember ?e1 .
                    %s
                    ?e1 aida:hasName ?label1 .
            } group by ?prototype ?label1
        }
    } group by ?prototype
}
''' % (open_clause, close_clause, open_clause, close_clause, open_clause, close_clause)


def proto_type(graph):
    open_clause = close_clause = ''
    if graph:
        open_clause = 'GRAPH <%s> {' % graph
        close_clause = '}'
    return '''
insert { 
    %s
    ?type_assertion a rdf:Statement ;
        rdf:subject ?prototype ;
        rdf:predicate rdf:type ;
        rdf:object ?t
    %s
}
where {
    {
        select ?prototype (max(?type1) as ?t)
        where {
          {
            select ?prototype (max(?cnt) as ?max)
            where {
              select ?prototype ?type (count(?type) as ?cnt)
              where {
                %s
                    ?c aida:prototype ?prototype .
                    ?membership aida:cluster ?c ; aida:clusterMember ?e .
                %s
                ?x a rdf:Statement ; rdf:subject ?e ; rdf:predicate rdf:type ; rdf:object ?type .
              } group by ?prototype ?type
            } group by ?prototype
          }
          {
            select ?prototype ?type1 (count(?type1) as ?max)
            where {
                %s
                    ?c1 aida:prototype ?prototype .
                    ?membership1 aida:cluster ?c1 ; aida:clusterMember ?e1 .
                %s
                ?x a rdf:Statement ; rdf:subject ?e1 ; rdf:predicate rdf:type ; rdf:object ?type1 .
            } group by ?prototype ?type1
          }
        } group by ?prototype
    }
    BIND( URI(CONCAT(STR(?prototype), "-type")) AS ?type_assertion )
}''' % (open_clause, close_clause, open_clause, close_clause, open_clause, close_clause)


def proto_justi(graph):
    open_clause = close_clause = ''
    if graph:
        open_clause = 'GRAPH <%s> {' % graph
        close_clause = '}'
    return '''
insert { 
    %s
    ?p aida:justifiedBy ?j .
    %s
}
where {
    %s
        ?c aida:prototype ?p .
        ?mem aida:cluster ?c ;
             aida:clusterMember ?x .
    %s
    ?x aida:justifiedBy ?j .
}
''' % (open_clause, close_clause, open_clause, close_clause)


def proto_type_assertion_justi(graph):
    open_clause = close_clause = ''
    if graph:
        open_clause = 'GRAPH <%s> {' % graph
        close_clause = '}'
    return '''
insert {
    %s
    ?r aida:justifiedBy ?j
    %s
    }
where {
    %s
    ?x aida:prototype ?proto .
    ?r a rdf:Statement ;
       rdf:subject ?proto ;
       rdf:predicate rdf:type ;
       rdf:object ?proto_type .
    ?proto aida:justifiedBy ?j .
    %s
}
''' % (open_clause, close_clause, open_clause, close_clause)


def super_edge(graph):
    uri_suffix = ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(16)])
    open_clause = close_clause = ''
    if graph:
        open_clause = 'GRAPH <%s> {' % graph
        close_clause = '}'
    return '''
insert { 
    %s
    [] a rdf:Statement ;
       rdf:subject ?evtRelProto ;
       rdf:predicate ?p ;
       rdf:object ?entProto ;
       aida:confidence [
            a aida:Confidence ;
            aida:confidenceValue ?cnt ;
            aida:system <http://www.isi.edu>
       ]
    %s
}
where {
  {
      select ?evtRelProto ?p ?entProto ((1 - (1/(2*count(*)))) as ?cnt)
      where {
          %s
              ?evtRelC aida:prototype ?evtRelProto .
              ?mem1 aida:cluster ?evtRelC ;
                   aida:clusterMember ?evtRel .
              ?entC aida:prototype ?entProto .
              ?mem2 aida:cluster ?entC ;
                   aida:clusterMember ?ent .
          %s
          ?state rdf:subject ?evtRel ;
                 rdf:predicate ?p ;
                 rdf:object ?ent .
      } groupby ?evtRelProto ?p ?entProto orderby desc(?cnt)
  }
  BIND( URI(CONCAT(STR(?p), "-%s")) AS ?type_assertion )
}
''' % (open_clause, close_clause, open_clause, close_clause, uri_suffix)


def super_edge_justif(graph):
    open_clause = close_clause = ''
    if graph:
        open_clause = 'GRAPH <%s> {' % graph
        close_clause = '}'
    return '''
insert {
    %s
    ?r aida:justifiedBy ?j .
    %s
} where {
    %s
    ?r a rdf:Statement ;
       rdf:subject ?evtRelProto ;
       rdf:predicate ?p ;
       rdf:object ?entProto .
    ?evtCluster aida:prototype ?evtRelProto .
    ?entCluster aida:prototype ?entProto .
    ?mem1 aida:cluster ?evtCluster;
          aida:clusterMember ?evt .
    ?mem2 aida:cluster ?entCluster;
          aida:clusterMember ?ent .
    %s
    ?r2 a rdf:Statement ;
       rdf:subject ?evt ;
       rdf:predicate ?p ;
       rdf:object ?ent ;
       aida:justifiedBy ?j .
}
    ''' % (open_clause, close_clause, open_clause, close_clause)
