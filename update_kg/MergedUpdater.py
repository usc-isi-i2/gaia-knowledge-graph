from Updater import Updater


class MergedUpdater(Updater):
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
        return ret



