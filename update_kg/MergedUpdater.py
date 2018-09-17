from .Updater import Updater
import json

class MergedUpdater(Updater):
    def get_json_head(self):
        ent_q = self.queries['merge_ent_nametypelink.sparql']
        res = self.select_bindings(ent_q)[1]
        for x in res:
            # ?e ?name ?translate ?type ?linkTarget
        # TODO: cluster
        pass

    def get_cluster(self):
        cluster = self.queries['merge_cluster.sparql']
        res = self.select_bindings(cluster)
        ret = {}
        for x in res:
            # ?c ?p ?e
            cluster_uri = x['c']['value']
            if cluster_uri not in ret:
                ret[cluster_uri] = [[], []]
            if 'p' in x:
                ret[cluster_uri][1].append(x['p']['value'])
            if 'e' in x:
                ret[cluster_uri][0].append(x['e']['value'])

        with open(self.outputs_prefix + 'cluster.json', 'w') as f:
            json.dump(ret, f, indent=2)


