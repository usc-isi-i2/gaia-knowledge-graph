import csv
from rdflib.graph import Graph
from SPARQLWrapper import SPARQLWrapper, CSV
from json_head.sparqls import *


class Selector(object):
    def __init__(self, endpoint: str):
        if endpoint.endswith('.ttl'):
            g = Graph()
            g.parse(endpoint, format='n3')
            self.graph = g
            self.run = self.select_query_rdflib
        else:
            self.sw = SPARQLWrapper(endpoint)
            self.sw.setReturnFormat(CSV)
            self.run = self.select_query_url

    def select_query_rdflib(self, q):
        csv_res = self.graph.query(PREFIX + q).serialize(format='csv')
        rows = [x.decode('utf-8') for x in csv_res.splitlines()][1:]
        return list(csv.reader(rows))

    def select_query_url(self, q):
        self.sw.setQuery(PREFIX + q)
        rows = self.sw.query().convert().decode('utf-8').splitlines()[1:]
        return list(csv.reader(rows))
