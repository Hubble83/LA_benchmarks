#!/bin/bash

#PBS -N POSTGRES_BENCHMARK
#PBS -l walltime=24:00:00
#PBS -l nodes=compute-781-1:ppn=64
#PBS -m abe
#PBS -M a71874@alunos.uminho.pt

# Specify libs directory
export LIB_DIR=$HOME/new_libs

# Specify lib subdirectories
export PG_DIR=$LIB_DIR/postgresql
export DSTAT_DIR=$LIB_DIR/dstat

# Specify data dir
export DATA_DIR=/share/jade/laolap_data

# Specify git clone directory
export REPO_DIR=$HOME

################################################################################

mkdir -p "results_postgres"

for q in 3 4 6 11 12 14
do
echo "dataset,k-best,average,std_dev" > "results_postgres/query_${q}_time.csv"
echo "dataset,k-best,average,std_dev" > "results_postgres/query_${q}_memory.csv"
done

for i in 1 2 4 8 16 32 64 128
do
# Start the server in background:
"$PG_DIR/postgresql/bin/pg_ctl" -D "$PG_DIR/postgresql/pgdata" -l "$PG_DIR/postgresql/postgresql.log" start &>> benchmark.log

# Wait server started
sleep 5

# Test the current dataset
for q in 3 4 6 11 12 14
do
    python "$REPO_DIR/VLDB_benchmarks/postgresql/postgresql.py" "$i" "$q" &>> benchmark.log
done
# Shutdown the server to free memory
"$PG_DIR/postgresql/bin/pg_ctl" -D "$PG_DIR/postgresql/pgdata" -l "$PG_DIR/postgresql/postgresql.log" stop &>> benchmark.log
sleep 5

done
