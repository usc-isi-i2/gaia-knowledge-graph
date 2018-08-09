"""
This file is used to generated a report for Entity Cluster analysis.

The output format is org-mode compatible, can be directly written in one,
and export with Emacs.
"""
from utils import query_with_wrapper, text_justify
from dot_utils import node_label_justify

endpoint = "http://localhost:3030/entity-split-cluster/sparql"


class ClusterReport:
    def __init__(self):
        self.cluster_count = {}
        self.mention_in = {}
        self.__init_cluster_count()
        self.__init_mention_in()

    def __init_cluster_count(self):
        query = """
PREFIX xij: <http://isi.edu/xij-rule-set#> 
SELECT ?cluster (COUNT(?member) AS ?memberN)
WHERE {
  ?member xij:inCluster ?cluster ;
}
GROUP BY ?cluster
        """
        for cluster, count in query_with_wrapper(endpoint, query):
            self.cluster_count[cluster] = count

    def __init_mention_in(self):
        query = """
PREFIX xij: <http://isi.edu/xij-rule-set#>
PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
SELECT ?cluster (COUNT(DISTINCT ?source) AS ?sourceN) (GROUP_CONCAT(DISTINCT ?source; separator = ", ") AS ?sources) 
WHERE {
  ?e xij:inCluster ?cluster ;
     a aida:Entity ;
     aida:justifiedBy ?justification .
  ?justification aida:source ?source .
}
GROUP BY ?cluster        """
        for cluster, source_n, sources in query_with_wrapper(endpoint, query):
            self.mention_in[cluster] = (source_n.toPython(), sources.toPython())

    def generate_report(self):
        query = """
PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
PREFIX xij: <http://isi.edu/xij-rule-set#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?sn1 ?prefLabel1 ?sn2 ?prefLabel2 ?predicate ?edgeCount (GROUP_CONCAT(?value; separator = ", ") AS ?sourceConf) (AVG(?value) AS ?avg)
WHERE {
  ?sn1 a aida:SameAsCluster ;
       aida:prototype ?prototype1 .
  ?prototype1 skos:prefLabel ?prefLabel1 .
  ?sn2 a aida:SameAsCluster ;
       aida:prototype ?prototype2 .
  ?prototype2 skos:prefLabel ?prefLabel2 .
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
GROUP BY ?sn1 ?prefLabel1 ?sn2 ?prefLabel2 ?predicate ?edgeCount
"""
        print("digraph G {")
        print('  node[style = "filled", fillcolor = "#edad56", color = "#edad56", penwidth = "3", label = ""]')
        print('  edge[color = "#2e3e56", penwidth = "2"]')
        supernodes = set()
        sources = set()
        for sn1, label1, sn2, label2, pred, count, confs, avg_conf in query_with_wrapper(endpoint, query):
            supernodes.add((sn1, label1))
            supernodes.add((sn2, label2))
            print('  "{}" -> "{}" [label="{}\\n(x{}, {:.3})"]'.format(sn1, sn2, pred.replace('http://darpa.mil/ontologies/SeedlingOntology#', ''), count, float(avg_conf)))
        for sn, label in supernodes:
            print('  "{}" [label="{}", tooltip="{}"]'.format(sn, node_label_justify(label, self.cluster_count[sn]), self.mention_in.get(sn, "")))
            if sn in self.mention_in:
                for source in self.mention_in[sn][1].split(', '):
                    sources.add(source)
                    print('  "{}" -> "{}" [label="", color="#ea555b"]'.format(sn, source))
        for source in sources:
            print('  "{}" [label="{}", fillcolor="#f2688d", color="#f2688d"]'.format(source, source))
        print("}")

report = ClusterReport()
report.generate_report()
