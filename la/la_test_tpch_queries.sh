#!/bin/bash

mkdir -p "results_la"

get_queries
for q in "${QUERIES[@]}"
do
    echo "dataset,k-best,average,std_dev" > "results_la/query_${q}_time.csv"
    echo "dataset,k-best,average,std_dev" > "results_la/query_${q}_memory.csv"
done

get_scales
for i in "${SF[@]}"
do
    # Test the current dataset
    python "$REPO_DIR/la/la.py" "$i"
done
