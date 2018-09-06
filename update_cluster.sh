#!/bin/bash

ENDPOINT="http://gaiadev01.isi.edu:3030/rpi0901aif80d2"
NT_FOLDER="/Users/dongyuli/isi/data/20180901/nt80d2"
JL_FOLDER="/Users/dongyuli/isi/data/20180901/jl80d2"

: "
steps = {
    'insert_hasname': lambda: send_query(q_insert_hasName),
    'delete_ori_cluster': lambda: send_query(q_delete_ori_cluster),
    'generate_cluster': generate_cluster,
    'insert_name_entity': lambda: send_query(q_insert_name_entity),
    'insert_type': lambda: send_query(q_insert_type),
    'insert_superEdge': lambda: send_query(q_insert_superedge)
}
"

date +"%R"
echo " - step 0: delete all data in the dataset !!!!!"
curl -X POST -d "update=delete {?s ?p ?o} where {?s ?p ?o}" "$ENDPOINT/update"
python update_cluster.py "count_triples" "$ENDPOINT"


date +"%R"
echo " - step 1: upload raw data"
for filename in $NT_FOLDER/*.nt; do
    if [ "$filename" != "$NT_FOLDER/phase1.nt" ]; then
        echo "   - uploading $filename ..."
        curl -X POST -d "@$filename" "$ENDPOINT/data" --header "Content-Type: text/turtle"
    fi
done
python update_cluster.py "count_triples" "$ENDPOINT"


date +"%R"
echo " - step 2.1: get json head for clustering"
python gaia-clustering/src/run.py "$ENDPOINT" "$JL_FOLDER"


date +"%R"
echo " - step 2.2: cluster entities"
/usr/bin/python2.7 gaia-clustering/multi_layer_network/test/from_jsonhead2cluster.py "$JL_FOLDER"

date +"%R"
echo " - step 2.3: cluster events"
/usr/bin/python2.7 gaia-clustering/multi_layer_network/test/baseline2.exe.py "$JL_FOLDER"


date +"%R"
echo " - step 3: delete orininal cluster and prototype"
python update_cluster.py "delete_ori_cluster" "$ENDPOINT"
python update_cluster.py "count_triples" "$ENDPOINT"


date +"%R"
echo " - step 4: generate clusters"
python update_cluster.py "generate_cluster" "$ENDPOINT" "$JL_FOLDER" "$NT_FOLDER"
python update_cluster.py "count_triples" "$ENDPOINT"


date +"%R"
echo " - step 5: upload clusters"
curl -X POST -d "@$NT_FOLDER/phase1.nt" "$ENDPOINT/data" --header "Content-Type: text/turtle"
python update_cluster.py "count_triples" "$ENDPOINT"



date +"%R"
echo "[TEMP]: generate relation cluster jl"
python gaia-clustering/src/generate_relation_cluster.py "$ENDPOINT" "$JL_FOLDER"

date +"%R"
echo "[TEMP]: generate relation cluster nt"
temp_generate_relation_cluster "$ENDPOINT" "$JL_FOLDER" "$NT_FOLDER"

date +"%R"
echo "[TEMP]: upload relation cluster nt"
curl -X POST -d "@$NT_FOLDER/relation.nt" "$ENDPOINT/data" --header "Content-Type: text/turtle"
python update_cluster.py "count_triples" "$ENDPOINT"




date +"%R"
echo " - step 6: insert name for entity prototypes"
python update_cluster.py "insert_name_entity" "$ENDPOINT"
python update_cluster.py "count_triples" "$ENDPOINT"


date +"%R"
echo " - step 7: insert type for prototypes"
python update_cluster.py "insert_type" "$ENDPOINT"
python update_cluster.py "count_triples" "$ENDPOINT"


date +"%R"
echo " - step 8: insert justifications for prototypes"
python update_cluster.py "insert_prototype_justification" "$ENDPOINT"
python update_cluster.py "count_triples" "$ENDPOINT"


date +"%R"
echo " - step 9: download the nt with clusters"
curl GET "$ENDPOINT/get" > "$JL_FOLDER/with_cluster.aif"


date +"%R"
echo " - step 10: insert superEdges"
python update_cluster.py "insert_superedge" "$ENDPOINT"
python update_cluster.py "count_triples" "$ENDPOINT"


date +"%R"
echo " - step 11: download the nt with clusters and superedges"
curl GET "$ENDPOINT/get" > "$JL_FOLDER/with_superedge.aif"


date +"%R"
