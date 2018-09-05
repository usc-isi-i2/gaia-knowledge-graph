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
    ?cluster a aida:SameAsCluster .
    ?cluster aida:prototype ?prototype .
    ?cluster aida:system ?system .
    
    ?clusterMembership a aida:ClusterMembership ;
        aida:cluster ?cluster ;
        aida:clusterMember ?clusterMember ;
        aida:confidence ?conf ;
        aida:system ?sys .
    ?conf a aida:Confidence ;
        aida:confidenceValue ?cv ;
        aida:system ?sys_conf
}
"""

q_insert_type = """
insert {
    [] a rdf:Statement ;
        rdf:subject ?prototype ;
        rdf:predicate rdf:type ;
        rdf:object ?t
    }
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

q_insert_hasName = """
insert {?e aida:hasName ?name}
where {
    select ?e (max(?label1) as ?name)
    where {
        {
            select ?e (max(?cnt) as ?max)
            where {
                select ?e ?label (count(?label) as ?cnt)
                where {
                    ?e a aida:Entity ;
                       aida:justifiedBy ?j .
                    ?j skos:prefLabel ?label .
                } group by ?e ?label
            } group by ?e
        }
        {
            select ?e ?label1 (count(?label1) as ?max)
            where {
                    ?e a aida:Entity ;
                       aida:justifiedBy ?j1 .
                    ?j1 skos:prefLabel ?label1 .
            } group by ?e ?label1
        }
    } group by ?e
}
"""

q_insert_prototype_justification = """
insert {
	?p aida:justifiedBy ?j
}
where {
  ?c a aida:SameAsCluster ;
     aida:prototype ?p .
  {select ?j where {
    ?mem aida:cluster ?c ;
         aida:clusterMember ?e .
    ?e aida:justifiedBy ?j .
    } limit 1}
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
    cluster_jsonls = [os.path.join(jl_path, filename) for filename in os.listdir(jl_path) if filename.endswith('.jl')]
    cm = ClusterMaker()
    cm.generate_cluster_nt(cluster_jsonls, output)


def send_query(q):
    endpoint = 'http://localhost:3030/rpi0901aida9979/update' if len(sys.argv) < 3 else sys.argv[2] + '/update'
    prefix = ''.join(['PREFIX %s: <%s>\n' % (abbr, full) for abbr, full in namespaces.items()])
    sw = SPARQLWrapper(endpoint=endpoint)
    sw.setQuery(prefix + q)
    sw.query()


def count_triples():
    q = "select (count(*) as ?cnt) where { ?s ?p ?o }"
    endpoint = 'http://localhost:3030/rpi0901aida9979/query' if len(sys.argv) < 3 else sys.argv[2] + '/query'
    prefix = ''.join(['PREFIX %s: <%s>\n' % (abbr, full) for abbr, full in namespaces.items()])
    sw = SPARQLWrapper(endpoint=endpoint)
    sw.setQuery(prefix + q)
    sw.setReturnFormat('json')
    print('  [COUNT TRIPLES] ', sw.query().convert()['results']['bindings'][0]['cnt']['value'])


steps = {
    'insert_hasname': lambda: send_query(q_insert_hasName),
    'delete_ori_cluster': lambda: send_query(q_delete_ori_cluster),
    'generate_cluster': generate_cluster,
    'insert_name_entity': lambda: send_query(q_insert_name_entity),
    'insert_type': lambda: send_query(q_insert_type),
    'insert_superEdge': lambda: send_query(q_insert_superedge),

    'count_triples': count_triples
}

# run
steps[sys.argv[1]]()
