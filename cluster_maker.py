"""
generate a .nt file from .jl cluster results
with:
    ?cluster a io:SameAsCluster .
    ?cluster io:prototype ?prototype .
    ?prototype a io:Event/io:Entity .
    ?membership a io:clusterMembership;
                io:cluster ?cluster;
                io:clusterMember ?entity/?event .
"""

from rdflib import URIRef
from rdflib.namespace import RDF
import json, os
from aif import aifutils, aida_rdf_ontologies
from aida_rdf_ontologies import AIDA_ANNOTATION

types = {
    'entities': AIDA_ANNOTATION.Entity,
    'events': AIDA_ANNOTATION.Event
}

# inputs:
jl_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'jl')
cluster_jsonls = [os.path.join(jl_path, filename) for filename in os.listdir(jl_path)]
output = 'nt/phase1.nt'
system = URIRef("http://isi.edu")


def make_cluster(g, cluster_uri, prototype_uri, entities, system):
    aifutils.make_cluster_with_prototype(g, cluster_uri, prototype_uri, system)
    for entity in map(URIRef, entities):
        aifutils.mark_as_possible_cluster_member(g, entity, cluster_uri, 1.0, system)
    return cluster_uri


def generate_cluster_nt(cluster_jsonls, output, system):
    all_cluster_uri = []
    g = aifutils.make_graph()
    for cluster_jsonl in cluster_jsonls:
        with open(cluster_jsonl) as f:
            for ln, line in enumerate(f.readlines()):
                json_object = json.loads(line)
                for type_, member_list in json_object.items():
                    prototype = URIRef(member_list[0]+'-prototype')
                    cluster_uri = URIRef(member_list[0]+'-cluster')
                    all_cluster_uri.append(cluster_uri)
                    make_cluster(g, cluster_uri, prototype, member_list, system)
                    g.add((prototype, RDF.type, types[type_]))
    g.serialize(output, "nt")
    return all_cluster_uri


all_cluster_uri = generate_cluster_nt(cluster_jsonls, output, system)


with open('all_cluster_uri.json', 'w') as f:
    json.dump(all_cluster_uri, f)
