# gaia-knowledge-graph
Tools to build knowledge graphs from multi-modal extractions

#### Generate AIF with clusters/superedges
1. set inputs in `update_cluster.sh`:
    ```
    ENDPOINT="http://your.fuseki.server:port/dateset_name"
    NT_FOLDER="/Path/to/the/folder/containing/AIF/from/TA1"
    JL_FOLDER="/Path/to/the/folder/containing/jl/cluster/results"
    ```
2. run `update_cluster.sh` to upload and generate the dataset with clusters and superedges

## Requirements 
- `rdflib`
- `SPARQLWrapper`
