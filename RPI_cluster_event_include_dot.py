"""
This file is used to generated a visualization for Clusters analysis.
Use graphviz to generate the graph with the output.

Update: 2018-08-04
"""
from dot_utils import node_color, node_label_justify, generate
from ClusterReport import ClusterReport


class AllClusterReport(ClusterReport):
    def __init__(self, endpoint):
        super().__init__(endpoint)
        self._init_cluster_count()
        self._init_cluster_label()
        self._init_cluster_type()
        self._init_cluster_source()
        self._init_cluster_tooltips()

    def read_superedges(self):
        node_strings = []
        edge_strings = []
        supernodes = set()
        for sn1, predicate, sn2, count, avg in self._query_super_edges():
            supernodes.add(sn1)
            supernodes.add(sn2)
            label = predicate.replace('http://darpa.mil/ontologies/SeedlingOntology#', '')
            edge = '  "{}" -> "{}" [label="{}\\n(×{}, {:.3})", color="#d62728", penwidth="2"]'.format(sn1, sn2, label, count, float(avg))
            edge_strings.append(edge)

        for sn in supernodes:
            label = node_label_justify(self.cluster_label[sn], self.cluster_count[sn])
            color = node_color(self.cluster_type[sn])
            if sn in self.cluster_tooltip:
                tooltip = ', tooltip="{}\\n{}"'.format(self.cluster_type[sn], self.cluster_tooltip[sn])
            else:
                tooltip = ', tooltip="{}"'.format(self.cluster_type[sn])
            node = '  "{}" [label="{}"{}{}]'.format(sn, label, tooltip, color)
            node_strings.append(node)

        return node_strings, edge_strings


report = AllClusterReport("http://kg2018a.isi.edu:3030/all_clusters/sparql")
nodes, edges = report.read_superedges()
dot = generate(nodes, edges)
with open('output/event_include.dot', 'w') as f:
    f.write(dot)

