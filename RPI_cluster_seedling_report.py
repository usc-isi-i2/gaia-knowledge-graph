"""
This file is used to generated a report for Entity Cluster analysis.

The output format is org-mode compatible, can be directly written in one,
and export with Emacs.
"""
from utils import query_with_wrapper

endpoint = "http://kg2018a.isi.edu:3030/clusters/sparql"


class ClusterReport:
    def __init__(self):
        self.cluster_labels = {}
        self.__init_cluster_labels()

    def __init_cluster_labels(self):
        query = """
PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
PREFIX xij: <http://isi.edu/xij-rule-set#> 
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?cluster ?label (COUNT(?label) AS ?labelN)
WHERE {
  ?cMembership a aida:ClusterMembership ;
               aida:cluster ?cluster ;
               aida:clusterMember ?member .
  ?member skos:prefLabel ?label .
}
GROUP BY ?cluster ?label 
ORDER BY DESC(?labelN) 
        """
        for cluster, label, count in query_with_wrapper(endpoint, query):
            self.cluster_labels[cluster] = self.cluster_labels.get(cluster, list())
            self.cluster_labels[cluster].append((label, count))

    def generate_report(self):
        query = """
PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
PREFIX xij: <http://isi.edu/xij-rule-set#> 
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?sn1 ?prefLabel1 ?sn2 ?prefLabel2 ?predicate ?edgeCount (GROUP_CONCAT(?value; separator = ", ") AS ?sourceConf)
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
  ?membership1 a aida:ClusterMembership ;
               aida:cluster ?sn1 ;
               aida:clusterMember ?e1 .
  ?membership2 a aida:ClusterMembership ;
               aida:cluster ?sn2 ;
               aida:clusterMember ?e2 .
  ?statement a rdf:Statement ;
             rdf:subject ?e1 ;
             rdf:predicate ?predicate ;
             rdf:object ?e2 ;
             aida:confidence ?confidence .
  ?confidence aida:confidenceValue ?value .
}
GROUP BY ?sn1 ?prefLabel1 ?sn2 ?prefLabel2 ?predicate ?edgeCount
ORDER BY DESC(?edgeCount)
"""
        for sn1, label1, sn2, label2, pred, count, conf in query_with_wrapper(endpoint, query):
            print("** {} {} {}".format(label1, pred.replace('http://darpa.mil/ontologies/SeedlingOntology#', 'domainOntology:'), label2))
            print("- *Cluster as Subject*")
            print("  - URI: {}".format(sn1))
            print("  - prefLabel: {}".format(label1))
            if self.cluster_labels[sn1]:
                print("  - labels:")
                for label, cnt in self.cluster_labels[sn1]:
                    print("    + ({}, {})".format(label, cnt))

            print("- *Cluster as Object*")
            print("  - URI: {}".format(sn2))
            print("  - prefLabel: {}".format(label2))
            if self.cluster_labels[sn2]:
                print("  - labels:")
                for label, cnt in self.cluster_labels[sn2]:
                    print("    + ({}, {})".format(label, cnt))

            print("- *Predicate*")
            print("  - URI: {}".format(pred))
            print("  - Count: {}".format(count))
            print("  - Confidence: {}".format(conf))

report = ClusterReport()
report.generate_report()