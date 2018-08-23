"""
1. upload original .nt file to fuseki
2. SPARQL-update-delete:
    ?cluster a aida:SameAsCluster .
    ?cluster aida:prototype ?prototype .
3. generate a .nt file from .jl cluster results
    with:
        ?cluster a aida:SameAsCluster .
        ?cluster aida:prototype ?prototype .
        ?prototype a aida:Event/aida:Entity .
        ?membership a aida:clusterMembership;
                    aida:cluster ?cluster;
                    aida:clusterMember ?entity/?event .
4. upload the .nt file step 3 created
5/6/7. SPARQL-update-insert, prototype.type/name:
    ?prototype a [max count rdf:type(exclude Entity/Event)] .
    ?prototype aida:hasName [max count aida:hasName] .
8. SPARQL-update-insert, SuperEdge:
    ?se a aida:SuperEdge ;
        rdf:subject ?sub_cluster ;
        rdf:predicate ?predicate ;
        rdf:object ?obj_cluster ;
        aida:edgeCount ?edge_count .
"""
import os, sys, json
from SPARQLWrapper import SPARQLWrapper

from rdflib import URIRef
from rdflib.namespace import RDF

from aif import aifutils, aida_rdf_ontologies
from aida_rdf_ontologies import AIDA_ANNOTATION

from namespaces import namespaces

q_delete_ori_cluster = """
DELETE {
    ?cluster a aida:SameAsCluster .
    ?cluster aida:prototype ?prototype .
    ?cluster aida:system ?system .
}
WHERE {
    ?cluster a aida:SameAsCluster .
    ?cluster aida:prototype ?prototype .
    ?cluster aida:system ?system .
}
"""

q_insert_type = """
insert {?prototype a ?t}
where {
    select ?prototype (max(?type1) as ?t)
    where {
      {
        select ?prototype (max(?cnt) as ?max)
        where {
          select ?prototype ?type (count(?type) as ?cnt)
          where {
            ?c aida:prototype ?prototype .
            ?x a rdf:Statement ; rdf:subject ?e ; rdf:predicate rdf:type ; rdf:object ?type .
            ?membership aida:cluster ?c ; aida:clusterMember ?e .
          } group by ?prototype ?type
        } group by ?prototype
      }
      {
        select ?prototype ?type1 (count(?type1) as ?max)
        where {
            ?c1 aida:prototype ?prototype .
            ?x a rdf:Statement ; rdf:subject ?e1 ; rdf:predicate rdf:type ; rdf:object ?type1 .
            ?membership1 aida:cluster ?c1 ; aida:clusterMember ?e1 .
        } group by ?prototype ?type1
      }
    } group by ?prototype
}
"""

q_insert_name_event = """
insert {?prototype aida:hasName ?name}
where {
    select ?prototype (max(?label1) as ?name)
    where {
        {
            select ?prototype (max(?cnt) as ?max)
            where {
                select ?prototype ?label (count(?label) as ?cnt)
                where {
                    ?c aida:prototype ?prototype .
                    ?e a aida:Event; aida:justifiedBy ?x . ?x skos:prefLabel ?label .
                    ?membership aida:cluster ?c ; aida:clusterMember ?e .
                } group by ?prototype ?label
            } group by ?prototype
        }
        {
            select ?prototype ?label1 (count(?label1) as ?max)
            where {
                ?c1 aida:prototype ?prototype .
                ?e1 a aida:Event; aida:justifiedBy ?x . ?x skos:prefLabel ?label1 .
                ?membership1 aida:cluster ?c1 ; aida:clusterMember ?e1 .
            } group by ?prototype ?label1
        }
    } group by ?prototype
}
"""

q_insert_name_entity = """
insert {?prototype aida:hasName ?name}
where {
    select ?prototype (max(?label1) as ?name)
    where {
        {
            select ?prototype (max(?cnt) as ?max)
            where {
                select ?prototype ?label (count(?label) as ?cnt)
                where {
                    ?c aida:prototype ?prototype .
                    ?e aida:hasName ?label .
                    ?membership aida:cluster ?c ; aida:clusterMember ?e .
                } group by ?prototype ?label
            } group by ?prototype
        }
        {
            select ?prototype ?label1 (count(?label1) as ?max)
            where {
                    ?c1 aida:prototype ?prototype .
                    ?e1 aida:hasName ?label1 .
                    ?membership1 aida:cluster ?c1 ; aida:clusterMember ?e1 .
            } group by ?prototype ?label1
        }
    } group by ?prototype
}
"""

q_insert_superedge = """
INSERT {
    [] a aida:SuperEdge;
        rdf:subject ?cluster1;
        rdf:predicate ?p;
        rdf:object ?cluster2;
        aida:edgeCount ?edgeN .
}
WHERE {
    SELECT ?cluster1 ?p ?cluster2 (COUNT(*) AS ?edgeN)
    WHERE {
        ?membership rdf:type aida:ClusterMembership ;
                    aida:cluster ?cluster1 ;
                    aida:clusterMember ?e1 .
        ?membership2 rdf:type aida:ClusterMembership ;
                    aida:cluster ?cluster2 ;
                    aida:clusterMember ?e2 .
        ?to_e1 a rdf:Statement ;
               rdf:subject ?relation ;
               rdf:predicate ?p1 ;
               rdf:object ?e1 .
        ?to_e2 a rdf:Statement ;
               rdf:subject ?relation ;
               rdf:predicate ?p2 ;
               rdf:object ?e2 .
        ?to_p a rdf:Statement ;
              rdf:subject ?relation ;
              rdf:predicate rdf:type ;
              rdf:object ?p .
        ?relation a aida:Relation .

        FILTER(
            ?p != aida:Relation
            && regex(str(?p1), 'subject$')
            && regex(str(?p2), 'object$')
        )
    }
    GROUP BY ?cluster1 ?p ?cluster2
}
"""

class ClusterMaker(object):
    def __init__(self):
        self.types = {
            'entities': AIDA_ANNOTATION.Entity,
            'events': AIDA_ANNOTATION.Event
        }
        self.system = URIRef("http://isi.edu")
        self.g = aifutils.make_graph()

    def generate_cluster_nt(self, cluster_jsonls, output):
        for cluster_jsonl in cluster_jsonls:
            with open(cluster_jsonl) as f:
                for ln, line in enumerate(f.readlines()):
                    json_object = json.loads(line)
                    for type_, member_list in json_object.items():
                        prototype_uri = URIRef(member_list[0]+'-prototype')
                        cluster_uri = URIRef(member_list[0]+'-cluster')

                        # ?cluster a aida:SameAsCluster .
                        # ?cluster aida:prototype ?prototype .
                        aifutils.make_cluster_with_prototype(self.g, cluster_uri, prototype_uri, self.system)

                        # ?prototype a aida:Event/aida:Entity .
                        self.g.add((prototype_uri, RDF.type, self.types[type_]))

                        # ?membership a aida:clusterMembership;
                        #             aida:cluster ?cluster;
                        #             aida:clusterMember ?member(entity/event) .
                        for member in map(URIRef, member_list):
                            aifutils.mark_as_possible_cluster_member(self.g, member, cluster_uri, 1.0, self.system)

        self.g.serialize(output, "nt")


def generate_cluster():
    jl_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'jl') if len(sys.argv) < 4 else sys.argv[3]
    output = 'nt/phase1.nt' if len(sys.argv) < 5 else sys.argv[4] + '/phase1.nt'
    cluster_jsonls = [os.path.join(jl_path, filename) for filename in os.listdir(jl_path)]
    cm = ClusterMaker()
    cm.generate_cluster_nt(cluster_jsonls, output)


def send_query(q):
    endpoint = 'http://localhost:3030/dryrun_all/update' if len(sys.argv) < 3 else sys.argv[2] + '/update'
    prefix = ''.join(['PREFIX %s: <%s>\n' % (abbr, full) for abbr, full in namespaces.items()])
    sw = SPARQLWrapper(endpoint=endpoint)
    sw.setQuery(prefix + q)
    sw.query()

# step 1: upload original .nt into Fuseki
# step 2: delete cluster/prototype in the original data
# step 3: generate a .nt file from .jl cluster results
# step 4: upload the .nt file that step 3 created into Fuseki
# step 5: insert prototype-name-entity
# step 6: insert prototype-name-event
# step 7: insert prototype-type
# step 8: insert SuperEdge


steps = {
    '2': lambda: send_query(q_delete_ori_cluster),
    '3': generate_cluster,
    '5': lambda: send_query(q_insert_name_entity),
    '6': lambda: send_query(q_insert_name_event),
    '7': lambda: send_query(q_insert_type),
    '8': lambda: send_query(q_insert_superedge)
}

# run
steps[sys.argv[1]]()
