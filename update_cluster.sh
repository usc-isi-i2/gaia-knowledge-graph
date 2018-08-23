#!/bin/bash

ENDPOINT="http://gaiadev01.isi.edu:3030/dryrun_all"
NT_FOLDER="/Users/dongyuli/isi/gaia_dryrun_data/Xin/nt"
JL_FOLDER="/Users/dongyuli/isi/gaia_dryrun_data/Xin/jl"

: '
date +"%R"
echo " - step 0: delete all data in the dataset !!!!!"
curl -X POST -d "update=delete {?s ?p ?o} where {?s ?p ?o}" "$ENDPOINT/update"
'

date +"%R"
echo " - step 1: upload raw data"
for filename in $NT_FOLDER/*.nt; do
    echo "   - uploading $filename ..."
    curl -X POST -d "@$filename" "$ENDPOINT/data" --header "Content-Type: text/turtle"
done

date +"%R"
echo " - step 2: delete orininal cluster and prototype"
python update_cluster.py 2 "$ENDPOINT"

date +"%R"
echo " - step 3: generate clusters"
python update_cluster.py 3 "$ENDPOINT" "$JL_FOLDER" "$NT_FOLDER"

date +"%R"
echo " - step 4: upload clusters"
curl -X POST -d "@$NT_FOLDER/phase1.nt" "$ENDPOINT/data" --header "Content-Type: text/turtle"

date +"%R"
echo " - step 5: insert name for entity prototypes"
python update_cluster.py 5 "$ENDPOINT"

date +"%R"
echo " - step 6: insert name for event prototypes"
python update_cluster.py 6 "$ENDPOINT"

date +"%R"
echo " - step 7: insert type for prototypes"
python update_cluster.py 7 "$ENDPOINT"

date +"%R"
