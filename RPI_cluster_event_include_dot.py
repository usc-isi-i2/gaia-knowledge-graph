"""
This file is used to generated a report for Entity Cluster analysis.

The output format is org-mode compatible, can be directly written in one,
and export with Emacs.
"""
from utils import query_with_wrapper, text_justify
from rdflib.namespace import split_uri

endpoint = "http://kg2018a.isi.edu:3030/all_clusters/sparql"


class ClusterReport:
    def __init__(self):
        self.cluster_count = {}
        self.cluster_label = {}
        self.cluster_type = {}
        self.cluster_source = {}
        self.__init_cluster_count()
        self.__init_cluster_label()
        self.__init_cluster_type()
        self.__init_cluster_source()

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

    def __init_cluster_label(self):
        query = """
PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT DISTINCT ?cluster ?label
WHERE {
  ?cluster aida:prototype ?prototype .
  ?prototype skos:prefLabel ?label
}
        """
        for cluster, label in query_with_wrapper(endpoint, query):
            self.cluster_label[cluster] = label

    def __init_cluster_type(self):
        query = """
PREFIX aida: <http://darpa.mil/aida/interchangeOntology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT DISTINCT ?cluster ?type
WHERE {
  ?cluster aida:prototype ?prototype .
  ?prototype a ?type .
  FILTER ( ?type NOT IN (aida:Entity, aida:Event ))
}
        """
        for cluster, type_ in query_with_wrapper(endpoint, query):
            _, type_name = split_uri(type_)
            self.cluster_type[cluster] = type_name

    def __init_cluster_source(self):
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
        for cluster, sources in query_with_wrapper(endpoint, query):
            self.cluster_source[cluster] = sources

    def read_superedges(self):
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
        supernodes = set()
        node_strings = []
        edge_strings = []
        for sn1, predicate, sn2, count, avg in query_with_wrapper(endpoint, query):
            supernodes.add(sn1)
            supernodes.add(sn2)
            label = predicate.replace('http://darpa.mil/ontologies/SeedlingOntology#', '')
            edge = '  "{}" -> "{}" [label="{}\\n(×{}, {:.3})", color="#d62728", penwidth="2"]'.format(sn1, sn2, label, count, float(avg))
            edge_strings.append(edge)

        for sn in supernodes:
            label = node_label_justify(self.cluster_label[sn], self.cluster_count[sn])
            color = node_color(self.cluster_type[sn])
            tooltip = ', tooltip="({}: {})"'.format(self.cluster_type[sn], self.cluster_source[sn])
            node = '  "{}" [label="{}"{}{}]'.format(sn, label, tooltip, color)
            node_strings.append(node)

        return node_strings, edge_strings


type_color_map = {
    # Entity
    'Facility': '#7f7f7f',
    'GeopoliticalEntity': '#e377c2',
    'Location': '#8c564b',
    'Organization': '#9467bd',
    'Person': '#1f77b4',
    'FillerType': '#ff7f0e'
    # Event
}


def node_color(type_):
    color = type_color_map.get(type_, '#17becf')
    return ', fillcolor="{}", color="{}"'.format(color, color)


def node_label_justify(label, count, max_width=20):
    words = label + " (×{})".format(count)
    return "\\n".join(text_justify(words, max_width))


def generate(node_strings, edge_strings):
    dot = [
        'digraph G {',
        '  node[style="filled"]',
    ]
    dot.extend(node_strings)
    dot.extend(edge_strings)
    dot.append('}')
    return '\n'.join(dot)


report = ClusterReport()
nodes, edges = report.read_superedges()
dot = generate(nodes, edges)
with open('event_include.dot', 'w') as f:
    f.write(dot)

