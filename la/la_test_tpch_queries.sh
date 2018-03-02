#!/bin/bash

#PBS -N LA_BENCHMARK
#PBS -l walltime=24:00:00
#PBS -l nodes=compute-781-1:ppn=64
#PBS -m abe
#PBS -M a71874@alunos.uminho.pt

# Specify libs directory
export LIB_DIR=$HOME/new_libs

# Specify lib subdirectories
export QUERIES_DIR=$HOME/PI/CPD_PI_HPC_OLAP/tpc_h_benchmark/laolap/bin
export DSTAT_DIR=$LIB_DIR/dstat

# Specify data dir
# export DATA_DIR=/share/jade/laolap_LA_data/data

# Specify git clone directory
export REPO_DIR=$HOME

################################################################################

mkdir -p "results_la"

for q in 3 4 6 11 12 14
do
echo "dataset,k-best,average,std_dev" > "results_la/query_${q}_time.csv"
echo "dataset,k-best,average,std_dev" > "results_la/query_${q}_memory.csv"
done

for i in 1 2 4 8 16 32 64 128
do
# Test the current dataset
python "$REPO_DIR/VLDB_benchmarks/la/la.py" "$i" &>> benchmark.log

done
