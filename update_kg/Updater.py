"""
After upload original data:
1.1 get entity json head
1.2 get event json head
1.3 cluster entity
1.4 cluster event
2.1 insert prototype hasName for entity
2.2 insert prototype type
2.3 insert prototype justification
DUMP KG IF NEEDED
3.1 insert SuperEdge
DUMP KG IF NEEDED
"""
import sys
import os
import json
import random
import string
from datetime import datetime
from SPARQLWrapper import SPARQLWrapper
from rdflib.plugins.stores.sparqlstore import SPARQLStore
import requests
mln_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../gaia-clustering/multi_layer_network/src')
sys.path.append(mln_path)
from namespaces import namespaces, ENTITY_TYPE_STR
mln_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../gaia-clustering/multi_layer_network/test')
sys.path.append(mln_path)
import baseline2_exe, from_jsonhead2cluster
sys.path.append(".")
from sparqls import *
import pandas as pd
from math import isnan


def divide_list_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

class Updater(object):
    def __init__(self, endpoint_src, endpoint_dst, name, outdir, graph, has_jl=False):
        if '3030' in endpoint_src:
            self.select_src = SPARQLWrapper(endpoint_src.rstrip('/') + '/query')
            self.update_src = SPARQLWrapper(endpoint_src.rstrip('/') + '/update')
            self.select_dst = SPARQLWrapper(endpoint_dst.rstrip('/') + '/query')
            self.update_dst = SPARQLWrapper(endpoint_dst.rstrip('/') + '/update')
            self.graphdb = False
        else:
            self.select_src = SPARQLWrapper(endpoint_src)
            self.update_src = SPARQLWrapper(endpoint_src)
            self.select_dst = SPARQLWrapper(endpoint_dst)
            self.update_dst = SPARQLWrapper(endpoint_dst)
            self.graphdb = True
        self.select_src.setReturnFormat('json')
        self.update_src.setMethod('POST')
        self.select_dst.setReturnFormat('json')
        self.update_dst.setMethod('POST')
        self.endpoint_src = endpoint_src.rstrip('/')
        self.endpoint_dst = endpoint_dst.rstrip('/')

        self.outdir = outdir
        self.name = name

        self.prefix = ''.join(['PREFIX %s: <%s>\n' % (abbr, full) for abbr, full in namespaces.items()])
        self.nt_prefix = ''.join(['@prefix %s: <%s> .\n' % (abbr, full) for abbr, full in namespaces.items()])
        self.system = 'http://www.isi.edu/TA2'

        self.entity_json = {}
        self.event_json = {}

        self.entity_jl = {}
        self.event_jl = {}
        self.relation_jl = {}

        self.graph = graph
        self.has_jl = has_jl

        self.all_entities = []
        self.all_events = []
        self.all_relations = []

        query = get_all_entities()
        for e in self.select_bindings(query, 'src')[1]:
            self.all_entities.append(e['e']['value'])

        query = get_all_events()
        for e in self.select_bindings(query, 'src')[1]:
            self.all_events.append(e['e']['value'])

        query = get_all_relations()
        for e in self.select_bindings(query, 'src')[1]:
            self.all_relations.append(e['e']['value'])
            # print(e['e']['value'])

    def run_delete_ori(self):
        delete_ori = delete_ori_cluster()
        print("start delete original clusters", datetime.now().isoformat())
        self.update_sparql(delete_ori)
        delete_ori_mem = delete_ori_clusterMember()
        print("start delete original clusters Memberships", datetime.now().isoformat())
        self.update_sparql(delete_ori_mem)
        print("Done. ", datetime.now().isoformat())

    def run_load_jl(self, entity_clusters='entity-clusters.jl',
                    event_clusters='event-clusters.jl',
                    relation_clusters='relation-clusters.jl'):
        if self.has_jl:
            print("start loading entity jl", datetime.now().isoformat())
            self.entity_jl = self.load_jl(self.outdir + '/' + entity_clusters)

            # create singleton clusters for entities without cluster
            print("create entity singletons", datetime.now().isoformat())
            count = 1
            for e in self.all_entities:
                if not any(e in x['members'] for cluster_id, x in self.entity_jl.items()):  # entity not in a cluster
                    print('\r', count, end='')
                    count += 1
                    self.entity_jl['%s-cluster' % e] = {'prototype': '%s-prototype' % e, 'members': [e]}
            print('')

            print("start loading event jl", datetime.now().isoformat())
            self.event_jl = self.load_jl(self.outdir + '/' + event_clusters)
            # create singleton clusters for events without cluster
            print("create event singletons", datetime.now().isoformat())
            count = 1
            for e in self.all_events:
                if not any(e in x['members'] for cluster_id, x in self.event_jl.items()):  # event not in a cluster
                    print('\r', count, end='')
                    count += 1
                    self.event_jl['%s-cluster' % e] = {'prototype': '%s-prototype' % e, 'members': [e]}
            print('')

            print("start getting relation jl", datetime.now().isoformat())
            if os.path.exists(self.outdir + '/' + relation_clusters):
                self.relation_jl = self.load_jl(self.outdir + '/' + relation_clusters)
            else:
                self.relation_jl = self.generate_relation_jl()
            # create singleton clusters for relations without clusters
            print("create relation singletons", datetime.now().isoformat())
            count = 1
            for e in self.all_relations:
                if not any(e in x['members'] for cluster_id, x in
                           self.relation_jl.items()):  # relation not in a cluster
                    print('\r', count, end='')
                    count += 1
                    self.relation_jl['%s-cluster' % e] = {'prototype': '%s-prototype' % e, 'members': [e]}
            print('')

        else:
            self.generate_jl()
        print("Done. ", datetime.now().isoformat())

    def run_system(self):
        print("start inserting system", datetime.now().isoformat())
        insert_system = system()
        self.update_sparql(insert_system)
        print("Done. ", datetime.now().isoformat())

    def run_entity_nt(self):
        print("start inserting triples for entity clusters", datetime.now().isoformat())
        entity_nt = self.convert_jl_to_nt(self.entity_jl, 'aida:Entity', 'entities')
        for chunk in divide_list_chunks(entity_nt, 1000):
            self.upload_data(chunk)
        print("Done. ", datetime.now().isoformat())

    def run_inf_just_nt(self):
        # print("start inserting triples for entity clusters informative justification", datetime.now().isoformat())
        # inf_just_nt = self.generate_cluster_inf_just_df(self.entity_jl, self.outdir + '/entity_informative_justification.csv')
        # for chunk in divide_list_chunks(inf_just_nt, 1000):
        #     self.upload_data(chunk)
        # print("Done. ", datetime.now().isoformat())
        #
        # print("start inserting triples for event clusters informative justification", datetime.now().isoformat())
        # inf_just_nt = self.generate_cluster_inf_just_df(self.event_jl,
        #                                                 self.outdir + '/event_informative_justification.csv')
        # for chunk in divide_list_chunks(inf_just_nt, 1000):
        #     self.upload_data(chunk)
        # print("Done. ", datetime.now().isoformat())
        #
        # print("start inserting triples for relation clusters informative justification", datetime.now().isoformat())
        # inf_just_nt = self.generate_cluster_inf_just_df(self.relation_jl,
        #                                                 self.outdir + '/relation_informative_justification.csv')
        # for chunk in divide_list_chunks(inf_just_nt, 1000):
        #     self.upload_data(chunk)
        # print("Done. ", datetime.now().isoformat())

        print("start inserting clusters informative justification", datetime.now().isoformat())
        insert_ij = insert_cluster_inf_just(self.graph)
        self.update_sparql(insert_ij)
        print("Done. ", datetime.now().isoformat())

    def run_links_nt(self):
        # print("start inserting triples for entity clusters links", datetime.now().isoformat())
        # links_nt = self.generate_entity_cluster_links_df(self.entity_jl, self.outdir + '/entity_links.csv')
        # for chunk in divide_list_chunks(links_nt, 1000):
        #     self.upload_data(chunk)
        # print("Done. ", datetime.now().isoformat())
        print("start inserting entity cluster links", datetime.now().isoformat())
        insert_ij = insert_cluster_links(self.graph)
        self.update_sparql(insert_ij)
        print("Done. ", datetime.now().isoformat())

    def run_event_nt(self):
        print("start inserting triples for event clusters", datetime.now().isoformat())
        event_nt = self.convert_jl_to_nt(self.event_jl, 'aida:Event', 'events')
        self.upload_data(event_nt)
        # if self.graphdb:
        #     input('upload nt and continue')
        print("Done. ", datetime.now().isoformat())

    def run_relation_nt(self):
        print("start inserting triples for relation clusters", datetime.now().isoformat())
        relation_nt = self.convert_jl_to_nt(self.relation_jl, 'aida:Relation', 'relations')
        self.upload_data(relation_nt)
        # if self.graphdb:
        #     input('upload nt and continue')
        print("Done. ", datetime.now().isoformat())

    def run_insert_proto(self):
        print("start inserting prototype name", datetime.now().isoformat())
        insert_name = proto_name(self.graph)
        self.update_sparql(insert_name)

        print("start inserting prototype type(category)", datetime.now().isoformat())
        insert_type = proto_type(self.graph)
        self.update_sparql(insert_type)

        print("start inserting prototype justification", datetime.now().isoformat())
        insert_justi = proto_justi(self.graph)
        self.update_sparql(insert_justi)

        print("start inserting prototype informative justification", datetime.now().isoformat())
        insert_justi = proto_inf_just(self.graph)
        self.update_sparql(insert_justi)

        print("start inserting prototype type-assertion justification", datetime.now().isoformat())
        insert_type_justi = proto_type_assertion_justi(self.graph)
        self.update_sparql(insert_type_justi)
        print("Done. ", datetime.now().isoformat())

    def run_super_edge(self):
        print("start inserting superEdge", datetime.now().isoformat())
        insert_se = super_edge(self.graph)
        self.update_sparql(insert_se)

        print("start inserting superEdge justifications", datetime.now().isoformat())
        insert_se_justi = super_edge_justif(self.graph)
        self.update_sparql(insert_se_justi)
        print("Done. ", datetime.now().isoformat())

    def get_json_head_entity(self):
        ent_q = get_entity()
        for x in self.select_bindings(ent_q, 'src')[1]:
            if 'linkTarget' in x:
                link_target = x['linkTarget']['value']
            else:
                link_target = self.random_str()
            name = x['name']['value'] if 'name' in x else ''
            txt_grained_type = []
            try:
                name = json.loads(x['translate']['value'])[0]
            except Exception:
                # TODO: get name from prefLabel ? if there is no hasName or textValue
                pass
            try:
                txt_grained_type = json.loads(x['txt_grained_type']['value'])
            except Exception:
                pass
            # TODO in cluster: make use of fine-grained types
            self.entity_json[x['e']['value']] = [name, x['type']['value'], link_target, txt_grained_type]

    def get_json_head_event(self):
        evt_q = get_event()
        for x in self.select_bindings(evt_q, 'src')[1]:
            evt_uri = x['e']['value']
            if evt_uri not in self.event_json:
                self.event_json[evt_uri] = {'type': x['type']['value'], 'doc': x['doc']['value'], 'text': []}
                for t in ENTITY_TYPE_STR:
                    self.event_json[evt_uri][t] = []

            cur = None
            try:
                cur = json.loads(x['translate']['value'])[0]
            except Exception:
                if 'text' in x:
                    cur = x['text']['value']
            if cur:
                self.event_json[evt_uri]['text'].append(cur)

            if 'ent' in x:
                ent = x['ent']['value']
                ent_name = self.entity_json[ent][0]
                ent_type = self.entity_json[ent][1]
                self.event_json[evt_uri][ent_type].append([ent, ent_name])

    # def generate_jl(self):
    #     print("start getting json head", datetime.now().isoformat())
    #     self.get_json_head_entity()
    #     self.get_json_head_event()
    #     print(len(self.entity_json), len(self.event_json))
    #
    #     # run Xin's clustering scripts
    #
    #     print("start getting entity jl", datetime.now().isoformat())
    #     self.entity_jl, entity_edgelist_G = from_jsonhead2cluster.run(self.entity_json, self.name)
    #     print("start getting event jl", datetime.now().isoformat())
    #     self.entity_jl = baseline2_exe.run(entity_edgelist_G, self.entity_json, self.event_json, self.name)

    def convert_jl_to_nt(self, jl, aida_type, key_type):
        res = []
        for cluster_uri, values in jl.items():
            prototype_uri = values['prototype']
            members = values['members']
            res.append(self.wrap_cluster(cluster_uri, prototype_uri, aida_type))
            memberships = '\n'.join([self.wrap_membership(cluster_uri, m) for m in members])
            memberships += '\n' + self.wrap_membership(cluster_uri, prototype_uri)
            res.append(memberships)
        return res

    def generate_cluster_inf_just_df(self, jl, dataframe):
        df_ij = pd.read_csv(dataframe)
        df_ij.where(pd.notnull(df_ij), None)
        res = []
        count = 1
        for cluster_uri, value in jl.items():
            print('\r', count, end='')
            count += 1
            ij_by_doc = {}  # ij for each doc with the highest confidence
            members = value['members']

            for member in members:  # each entity only has 1 informative justification
                df_ij_m = df_ij.loc[df_ij['entity'] == member]
                for index, x in df_ij_m.iterrows():
                    doc_id = x['just_doc']
                    conf = float(x['just_confidence_value'])
                    if doc_id not in ij_by_doc or conf > ij_by_doc[doc_id]['cv']:
                        ij_by_doc[doc_id] = {'cv': conf, 'ij': x}

            for _, x in ij_by_doc.items():
                ij = x['ij']
                just_type = ij['just_type']
                just_doc = ij['just_doc']
                just_source = ij['just_source']
                just_cv = ij['just_confidence_value']
                if just_type == 'aida:TextJustification':  # text justification
                    just_so = int(ij['so'])
                    just_eo = int(ij['eo'])
                    inf_just = '''
                        <%s> aida:informativeJustification [
                            a %s ;
                            aida:source "%s" ;
                            aida:sourceDocument "%s" ;
                            aida:confidence [
                                a aida:Confidence ;
                                aida:confidenceValue  "%s"^^xsd:double ;
                                aida:system <%s> ] ;
                            aida:startOffset  "%s"^^xsd:int;
                            aida:endOffsetInclusive "%s"^^xsd:int ;
                            aida:system <%s> ] .
                    ''' % (cluster_uri, just_type, just_source, just_doc, just_cv, self.system, just_so, just_eo, self.system)
                    res.append(inf_just)
                elif just_type == 'aida:KeyFrameVideoJustification':  # video justification
                    just_kfid = ij['kfid']
                    just_ulx = int(ij['ulx'])
                    just_uly = int(ij['uly'])
                    just_lrx = int(ij['lrx'])
                    just_lry = int(ij['lry'])
                    inf_just = '''
                        <%s> aida:informativeJustification [
                            a %s ;
                            aida:source "%s" ;
                            aida:sourceDocument "%s" ;
                            aida:confidence [
                                a aida:Confidence ;
                                aida:confidenceValue  "%s"^^xsd:double ;
                                aida:system <%s> ] ;
                            aida:keyFrame "%s" ;
                            aida:boundingBox [
                                aida:boundingBoxUpperLeftX  "%s"^^xsd:int ;
                                aida:boundingBoxUpperLeftY  "%s"^^xsd:int ;
                                aida:boundingBoxLowerRightX "%s"^^xsd:int ;
                                aida:boundingBoxLowerRightY "%s"^^xsd:int ;
                                rdf:type aida:BoundingBox ;
                                aida:system <%s> ];
                            aida:system <%s> ] .
                    ''' % (cluster_uri, just_type, just_source, just_doc, just_cv, self.system, just_kfid, just_ulx,
                           just_uly, just_lrx, just_lry, self.system, self.system)
                    res.append(inf_just)
                elif just_type == 'aida:ShotVideoJustification':  # short video justification
                    just_sid = ij['sid']
                    inf_just = '''
                        <%s> aida:informativeJustification [
                            a %s ;
                            aida:source "%s" ;
                            aida:sourceDocument "%s" ;
                            aida:confidence [
                                a aida:Confidence ;
                                aida:confidenceValue  "%s"^^xsd:double ;
                                aida:system <%s> ] ;
                            aida:aida:shot  "%s";
                            aida:system <%s> ] .
                    ''' % (cluster_uri, just_type, just_source, just_doc, just_cv, self.system, just_sid, self.system)
                    res.append(inf_just)
                elif just_type == 'aida:AudioJustification':  # audio justification
                    just_st = ij['st']
                    just_et = ij['et']
                    inf_just = '''
                        <%s> aida:informativeJustification [
                            a %s ;
                            aida:source "%s" ;
                            aida:sourceDocument "%s" ;
                            aida:confidence [
                                a aida:Confidence ;
                                aida:confidenceValue  "%s"^^xsd:double ;
                                aida:system <%s> ] ;
                            aida:aida:startTimestamp  "%s";
                            aida:aida:endTimestamp  "%s";
                            aida:system <%s> ] .
                    ''' % (cluster_uri, just_type, just_source, just_doc, just_cv, self.system, just_st, just_et, self.system)
                    res.append(inf_just)
                elif just_type == 'aida:ImageJustification':  # image justification
                    just_ulx = int(ij['ulx']) if not isnan(ij['ulx']) else 0
                    just_uly = int(ij['uly']) if not isnan(ij['uly']) else 0
                    just_lrx = int(ij['lrx']) if not isnan(ij['lrx']) else 0
                    just_lry = int(ij['lry']) if not isnan(ij['lry']) else 0
                    inf_just = '''
                        <%s> aida:informativeJustification [
                            a %s ;
                            aida:source "%s" ;
                            aida:sourceDocument "%s" ;
                            aida:confidence [
                                a aida:Confidence ;
                                aida:confidenceValue  "%s"^^xsd:double ;
                                aida:system <%s> ] ;
                            aida:boundingBox [
                                aida:boundingBoxUpperLeftX  "%s"^^xsd:int ;
                                aida:boundingBoxUpperLeftY  "%s"^^xsd:int ;
                                aida:boundingBoxLowerRightX "%s"^^xsd:int ;
                                aida:boundingBoxLowerRightY "%s"^^xsd:int ;
                                rdf:type aida:BoundingBox ;
                                aida:system <%s> ] ;
                            aida:system <%s> ] .
                    ''' % (cluster_uri, just_type, just_source, just_doc, just_cv, self.system, just_ulx,
                           just_uly, just_lrx, just_lry, self.system, self.system)
                    res.append(inf_just)
        print('')
        return res

    def generate_entity_cluster_links_df(self, jl, dataframe):
        df_links = pd.read_csv(dataframe)
        res = []
        count = 1
        print('')
        for cluster_uri, value in jl.items():
            print('\r', count, end='')
            count += 1
            members = value['members']
            targets_cv = {}  # link target and highgest confidence value
            for member in members:
                df_links_m = df_links.loc[df_links['entity'] == member]
                for index, x in df_links_m.iterrows():
                    link_target = x['link_target']
                    cv = x['link_cv']
                    if link_target not in targets_cv:
                        targets_cv[link_target] = cv
                    elif float(cv) > float(targets_cv[link_target]):
                        targets_cv[link_target] = cv
            for link_target, cv in targets_cv.items():
                link = '''
                    <%s> aida:link [
                        a aida:LinkAssertion ;
                        aida:linkTarget "%s" ;
                        aida:confidence [
                            a aida:Confidence ;
                            aida:confidenceValue  "%s"^^xsd:double ;
                            aida:system <%s> ] ;
                        aida:system <%s> ] .
                ''' % (cluster_uri, link_target, cv, self.system, self.system)
                res.append(link)
        print('')
        return res

    def generate_relation_jl(self):
        groups = {}
        rel_json = {}
        q = get_relation(self.graph)
        for x in self.select_bindings(q, 'dst')[1]:
            rel_uri = x['e']['value']
            if rel_uri not in rel_json:
                rel_json[rel_uri] = {'links': [], 'type': x['type']['value'].rsplit('#', 1)[-1]}

                pred = x['p']['value']
                cluster = x['cluster']['value']
                rel_json[rel_uri]['links'].append((pred.rsplit('#', 1)[-1], cluster.rsplit('#', 1)[-1]))
        for rel, attrs in rel_json.items():
            attr = attrs['type'] + str(sorted(attrs['links']))
            if attr not in groups:
                groups[attr] = []
            groups[attr].append(rel)

        jl = [v for v in groups.values()]
        with open(self.outdir + '/relation-clusters.jl', 'w') as f:
            for line in jl:
                json.dump(line, f)
                f.write('\n')
        jl = self.load_jl(self.outdir + '/relation-clusters.jl')
        return jl

    def select_bindings(self, q, origin):
        select = self.select_src if origin == 'src' else self.select_dst
        if self.graphdb:
            select.setQuery(self.prefix + q)
        else:
            select.setQuery(self.prefix + q)
        ans = select.query().convert()
        return ans['head']['vars'], ans['results']['bindings']

    def update_sparql(self, q):
        if self.graphdb:
            req_ = requests.post(self.endpoint_dst + '/statements', params={'update': self.prefix + q})
            print(req_, req_.content)
        else:
            self.update_dst.setQuery(self.prefix + q)
            print('  ', self.update_dst.query().convert())

    def upload_data(self, triple_list):
        data = self.nt_prefix + '\n'.join(triple_list)
        if self.graphdb:
            if self.graph:
                #     print('!!! not support graph now -- will insert into default graph !!!')
                ep = self.endpoint_dst + '/rdf-graphs/service?graph=' + self.graph
            else:
                ep = self.endpoint_dst + '/statements'
        else:
            ep = self.endpoint_dst + '/data'
            if self.graph:
                ep += ('?graph=' + self.graph)
        print('  start a post request on %s, with triple list length %d' % (ep, len(triple_list)))
        # r = requests.post(ep, data=data, headers={'Content-Type': 'text/turtle'})
        r = requests.post(ep, data=data.encode('utf-8'), headers={'Content-Type': 'text/turtle; charset=utf-8'})
        print('  response ', r.content)
        return r.content

    def wrap_membership(self, cluster, member):
        membership = '''
        [] a aida:ClusterMembership ;
           aida:cluster <%s> ;
           aida:clusterMember <%s> ;
           aida:confidence [
                a aida:Confidence ;
                aida:confidenceValue  "1.0"^^xsd:double ;
                aida:system <%s> ] ;
           aida:system <%s> .
        ''' % (cluster, member, self.system, self.system)
        return membership.strip('\n')

    def wrap_cluster(self, cluster, prototype, prototype_type):
        cluster = '''
        <%s> a               aida:SameAsCluster ;
             aida:prototype  <%s> ;
             aida:system     <%s> .
        <%s> a %s .
        ''' % (cluster, prototype, self.system, prototype, prototype_type)
        return cluster.strip('\n')

    @staticmethod
    def load_jl(file_path):
        jl = {}
        count = 1
        with open(file_path) as f:
            for line in f.readlines():
                print('\r', count, end='')
                count += 1
                members = json.loads(line)
                id = '%s-cluster' % members[0]
                if id in jl:
                    random = Updater.random_str(6)
                    id = '%s-cluster-%s' % (members[0], random)
                    proto = '%s-prototype-%s' % (members[0], random)
                else:
                    id = '%s-cluster' % members[0]
                    proto = '%s-prototype' % members[0]
                jl[id] = {'prototype': proto, 'members': members}
            print('')
        return jl

    @staticmethod
    def random_str(length=32):
        return ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(length)])
