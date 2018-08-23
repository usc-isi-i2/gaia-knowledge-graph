from utils import query_with_wrapper
from dot_utils import node_color, node_label_justify, generate
from rdflib.namespace import split_uri
from collections import defaultdict
from RPI_cluster_source_parse import SourceContext


class ClusterReport:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self._init_cluster_type()
        self._init_cluster_count()
        self._init_cluster_label()

    def _init_cluster_tooltips(self):
        cluster_tooltip = defaultdict(list)
        contextor = SourceContext()
        query = """
    PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
    PREFIX xij: <http://isi.edu/xij-rule-set#>
    SELECT DISTINCT ?cluster ?source ?start ?end
    WHERE {
      ?entity xij:inCluster ?cluster ;
              aida:justifiedBy ?justification .
      ?justification aida:source ?source ;
                     aida:startOffset ?start ;
                     aida:endOffsetInclusive ?end .
    }
    ORDER BY ?source ?start
        """
        for cluster, source, start, end in query_with_wrapper(self.endpoint, query):
            context = contextor.get_some_context(source.toPython(), start.toPython(), end.toPython())
            if context:
                context = '{}: {}'.format(source, context)
                cluster_tooltip[cluster].append(context)

        self.cluster_tooltip = {}
        for cluster in cluster_tooltip:
            tooltip = '\\n'.join(cluster_tooltip[cluster])
            self.cluster_tooltip[cluster] = tooltip.replace('"', '\\"')

    def _init_cluster_count(self):
        self.cluster_count = {}
        query = """
    PREFIX xij: <http://isi.edu/xij-rule-set#> 
    SELECT ?cluster (COUNT(?member) AS ?memberN)
    WHERE {
      ?member xij:inCluster ?cluster ;
    }
    GROUP BY ?cluster
            """
        for cluster, count in query_with_wrapper(self.endpoint, query):
            self.cluster_count[cluster] = count

    def _init_cluster_label(self):
        self.cluster_label = {}
        query = """
   PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
   SELECT DISTINCT ?cluster ?label
   WHERE {
     ?cluster aida:prototype ?prototype .
     ?prototype skos:prefLabel ?label
   }
           """
        for cluster, label in query_with_wrapper(self.endpoint, query):
            self.cluster_label[cluster] = label

    def _init_cluster_type(self):
        self.cluster_type = {}
        query = """
   PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
   SELECT DISTINCT ?cluster ?type
   WHERE {
     ?cluster aida:prototype ?prototype .
     ?prototype a ?type .
     FILTER ( ?type NOT IN (aida:Entity, aida:Event ))
   }
           """
        for cluster, type_ in query_with_wrapper(self.endpoint, query):
            _, type_name = split_uri(type_)
            self.cluster_type[cluster] = type_name

    def _init_cluster_source(self):
        self.cluster_source = {}
        query = """
   PREFIX xij: <http://isi.edu/xij-rule-set#>
   PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
   SELECT ?cluster (GROUP_CONCAT(DISTINCT ?source; separator = ", ") AS ?sources) 
   WHERE {
     ?e xij:inCluster ?cluster ;
        aida:justifiedBy ?justification .
     ?justification aida:source ?source .
   }
   GROUP BY ?cluster 
           """
        for cluster, sources in query_with_wrapper(self.endpoint, query):
            self.cluster_source[cluster] = sources

    def _query_super_edges(self):
        query = """
PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xij: <http://isi.edu/xij-rule-set#>
SELECT ?sn1 ?predicate ?sn2 ?edgeCount (AVG(?value) AS ?avg)
WHERE {
  ?se a aida:SuperEdge ;
      rdf:subject ?sn1 ;
      rdf:object ?sn2 ;
      rdf:predicate ?predicate ;
      aida:edgeCount ?edgeCount .
  ?e1 xij:inCluster ?sn1 .
  ?e2 xij:inCluster ?sn2 .
  ?statement a rdf:Statement ;
             rdf:subject ?e1 ;
             rdf:predicate ?predicate ;
             rdf:object ?e2 ;
             aida:confidence ?confidence .
  ?confidence aida:confidenceValue ?value .
}
GROUP BY ?sn1 ?predicate ?sn2 ?edgeCount
"""
        for sn1, predicate, sn2, count, avg in query_with_wrapper(self.endpoint, query):
            pred = predicate.replace('http://darpa.mil/ontologies/SeedlingOntology#', '')
            yield sn1, pred, sn2, int(count), float(avg)

    def _query_entity_super_edges(self):
        query = """
PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xij: <http://isi.edu/xij-rule-set#>
SELECT ?sn1 ?predicate ?sn2 ?edgeCount (AVG(?value) AS ?avg)
WHERE {
  ?sn1 aida:prototype ?prototype1 .
  ?prototype1 a aida:Entity .
  ?sn2 aida:prototype ?prototype2 .
  ?prototype2 a aida:Entity .
  ?se a aida:SuperEdge ;
      rdf:subject ?sn1 ;
      rdf:object ?sn2 ;
      rdf:predicate ?predicate ;
      aida:edgeCount ?edgeCount .
  ?e1 xij:inCluster ?sn1 .
  ?e2 xij:inCluster ?sn2 .
  ?statement a rdf:Statement ;
             rdf:subject ?e1 ;
             rdf:predicate ?predicate ;
             rdf:object ?e2 ;
             aida:confidence ?confidence .
  ?confidence aida:confidenceValue ?value .
}
GROUP BY ?sn1 ?label1 ?predicate ?sn2 ?label2 ?edgeCount
"""
        for sn1, predicate, sn2, count, avg in query_with_wrapper(self.endpoint, query):
            pred = predicate.replace('http://darpa.mil/ontologies/SeedlingOntology#', '')
            yield sn1, pred, sn2, int(count), float(avg)
