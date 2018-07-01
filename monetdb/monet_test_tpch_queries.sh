#!/bin/bash -x

mkdir -p "results_monetdb"

get_queries
for q in "${QUERIES[@]}"
do
    echo "dataset,k-best,average,std_dev" > "results_monetdb/query_${q}_time.csv"
    echo "dataset,k-best,average,std_dev" > "results_monetdb/query_${q}_memory.csv"
done

get_scales
for i in "${SF[@]}"
do
    # Start the server in background:
    # "$MONET_DIR/monetdb/bin/monetdbd" start my-dbfarm
    # Wait server started
    # sleep 5

    # Test the current dataset
    for q in "${QUERIES[@]}"
    do
        python "$REPO_DIR/monetdb/monetdb.py" "$i" "$q"
    done

    # Shutdown the server to free memory
    # "$MONET_DIR/monetdb/bin/monetdbd" stop my-dbfarm
    # sleep 5

done
