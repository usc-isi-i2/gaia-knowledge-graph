
import json
from datetime import datetime
from pathlib import Path
import sys
sys.path.append('../')
from json_head.Selector import Selector
from json_head.sparqls import *

DUMMY = 0


def get_ent(select, ent_json):
    global DUMMY
    res = select(ent_type_link)
    for line in res:
        ent, _type, lt = line
        if not lt:
            lt = 'DUMMY:%d' % DUMMY
            DUMMY += 1
        ent_json[ent] = ['', _type, lt]

    res = select(ent_name)
    for line in res:
        e, name, translate = line
        if translate:
            name = translate
        if e not in ent_json:
            ent_json[e] = ['', '', 'DUMMY:%d' % DUMMY]
            DUMMY += 1
        ent_json[e][0] = name


def get_cluster(select, cluster_json):
    res = select(cluster)
    for line in res:
        e, c = line
        if c not in cluster_json:
            cluster_json[c] = [[], []]
        cluster_json[c][0].append(e)

    res = select(proto)
    for line in res:
        c, p = line
        if c not in cluster_json:
            cluster_json[c] = [[], []]
        cluster_json[c][1].append(p)


def get_evt(select, evt_json):
    res = select(evt)
    for line in res:
        evt_uri, _type, doc, text, translate, ent = line
        if translate:
            text = translate
        if evt_uri not in evt_json:
            evt_json[evt_uri] = {'type': _type, 'doc': doc, 'text': [], 'entities': []}
        if text:
            evt_json[evt_uri]['text'].append(text)
        if ent:
            evt_json[evt_uri]['entities'].append(ent)


def dump(output, obj):
    print('start dump ', output, str(datetime.now()))
    with open(output, 'w') as f1:
        json.dump(obj, f1, indent=2)
    print('Done')


def get_all(ep, output):
    ent_json, cluster_json, evt_json = {}, {}, {}

    selector = Selector(ep)
    select = selector.run

    print('start query ent type link', str(datetime.now()))
    get_ent(select, ent_json)
    dump(output + 'entity.json', ent_json)

    print('start query cluster', str(datetime.now()))
    get_cluster(select, cluster_json)
    dump(output + 'cluster.json', cluster_json)

    print('start fetch evt', str(datetime.now()))
    get_evt(select, evt_json)
    dump(output + 'event.json', evt_json)


def get_one_by_one(ttl_path, output):
    ent_json, cluster_json, evt_json = {}, {}, {}
    ttls = [str(_) for _ in ttl_path.glob('*.ttl')]
    cnt = 0
    total = len(ttls)
    per = total // 100 if total > 100 else 1
    with open(output + 'fail.log', 'w') as f:
        for ttl in ttls:
            if cnt % per == 0:
                print('loading ... %d of %d' % (cnt, total), str(datetime.now()))
            cnt += 1
            try:
                selector = Selector(ttl)
                select = selector.run

                get_ent(select, ent_json)
                get_cluster(select, cluster_json)
                get_evt(select, evt_json)
            except:
                f.write(ttl)
                f.write('\n')

    dump(output + 'entity.json', ent_json)
    dump(output + 'cluster.json', cluster_json)
    dump(output + 'event.json', evt_json)
    print('ALL DONE', str(datetime.now()))


_, ep, out = sys.argv

if ep.startswith('http'):
    get_all(ep, out)
else:
    get_one_by_one(Path(ep), out)
