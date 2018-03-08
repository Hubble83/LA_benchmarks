#!/bin/bash

mkdir -p "results_postgres"

get_queries
for q in "${QUERIES[@]}"
do
    echo "dataset,k-best,average,std_dev" > "results_postgres/query_${q}_time.csv"
    echo "dataset,k-best,average,std_dev" > "results_postgres/query_${q}_memory.csv"
done

get_scales
for i in "${SF[@]}"
do
    # Start the server in background:
    "$PG_DIR/postgresql/bin/pg_ctl" -D "$PG_DIR/postgresql/pgdata" -l "$PG_DIR/postgresql/postgresql.log" start

    # Wait server started
    sleep 5

    # Test the current dataset
    for q in "${QUERIES[@]}"
    do
        python "$REPO_DIR/postgresql/postgresql.py" "$i" "$q"
    done
    # Shutdown the server to free memory
    "$PG_DIR/postgresql/bin/pg_ctl" -D "$PG_DIR/postgresql/pgdata" -l "$PG_DIR/postgresql/postgresql.log" stop
    sleep 5

done
