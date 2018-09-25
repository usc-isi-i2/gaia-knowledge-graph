def get_q(graph='http://www.isi.edu'):
    if graph:
        return '''SELECT distinct ?e ?type ?p ?cluster
        WHERE {
            ?e a aida:Relation .
            ?r rdf:subject ?e ;
               rdf:predicate rdf:type ;
               rdf:object ?type .
    
            ?r2 rdf:subject ?e ;
               rdf:predicate ?p ;
               rdf:object ?ent .
            GRAPH <%s> {
                ?mem aida:cluster ?cluster ;
                    aida:clusterMember ?ent .
            }
        }
        ''' % graph
    else:
        return '''SELECT distinct ?e ?type ?p ?cluster
        WHERE {
            ?e a aida:Relation .
            ?r rdf:subject ?e ;
               rdf:predicate rdf:type ;
               rdf:object ?type .
    
            ?r2 rdf:subject ?e ;
               rdf:predicate ?p ;
               rdf:object ?ent .
            ?mem aida:cluster ?cluster ;
               aida:clusterMember ?ent .
        }
        '''
