import rdflib
import json
import requests
import pycurl
import urllib
'''
with open("C:\\Users\\anika_000\\Desktop\\myLocalRDF.nt.txt") as f:
    content = f.read()
content = "@prefix : <http://example.org/#> . :a :b :c .".encode()
url = "http://rdf-translator.appspot.com/convert/n3/nt"

#print content
response = requests.post(url, data=content)
print response.text

'''
g = rdflib.Graph()
# ... add some triples to g somehow ...
g.parse("output1.nt", format="n3")
qres = g.query(
"""SELECT ?sub ?pred ?obj
WHERE {
  ?statement rdf:subject ?sub.
  ?statement rdf:predicate ?pred.
  ?statement rdf:object ?obj.
}""")
#for r in qres:
    #print("%s %s %s" % r)
#    print r["sub"] + " " + r["pred"] + " " + r["obj"]
dict = {}
for r in qres:
    if r["sub"] in dict.keys():
        currContent = dict[r["sub"]]
        if r["pred"] in currContent.keys():
            currList = currContent[r["pred"]]
            currList.append(r["obj"])
        else:
            newList = []
            newList.append(r["obj"])
            currContent[r["pred"]] = newList
    else:
        newDict = {}
        listObj = []
        listObj.append(r["obj"])
        newDict[r["pred"]] = listObj
        dict[r["sub"]] = newDict


fo = open("output1.json", "w")
fo.write(json.dumps(dict, indent=4))

