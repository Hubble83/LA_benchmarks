#!/bin/bash

#PBS -N LA_BENCHMARK
#PBS -l walltime=24:00:00
#PBS -l nodes=compute-781-2:ppn=64
#PBS -m abe
#PBS -M a71874@alunos.uminho.pt

# Specify libs directory
export LIB_DIR="$HOME/la_benchmark_libs"

# Specify LA data dir
DATA_DIR="$LIB_DIR/data"


function get_scales() {
    # Specify datasets to test (TPC-H scale factors)
    SF=(1 2) # 2 4 8 16 32 64
}

function get_queries() {
    # Specify queries to test
    QUERIES=(4 6 11) #(3 4 6 11 12 14)
}

# Specify path to dbgen
DBGEN_DIR="$HOME/PI/tpch_2_17_0/dbgen"

# Specify number of threads used by make
export MAKE_NUM_THREADS=64

# Specify total RAM in GB
export RAM_SIZE=256

# Add m4 to PATH
PATH=$PATH:/usr/bin/m4


###############################################################################


export -f get_scales
export -f get_queries

export DSTAT_DIR=$LIB_DIR/dstat
export LA_DATA_DIR="$DATA_DIR/la"
export DBGEN_DATA_DIR="$DATA_DIR/dbgen"
mkdir -p "$DATA_DIR $LA_DATA_DIR $DBGEN_DATA_DIR" || exit

export BOOST_DIR=$LIB_DIR/boost
export BISON_DIR=$LIB_DIR/bison
export MYSQL_DIR=$LIB_DIR/mysql
export PG_DIR=$LIB_DIR/postgresql
export DSTAT_DIR=$LIB_DIR/dstat
export READLINE_DIR=$LIB_DIR/readline
mkdir -p "$BOOST_DIR $BISON_DIR $MYSQL_DIR $PG_DIR $DSTAT_DIR $READLINE_DIR" || exit

export MY_CNF="$MYSQL_DIR/mysql/my.cnf"
export QUERIES_DIR="$REPO_DIR/la/bin"

REPO_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export REPO_DIR

get_queries
QUERIES_STRING=$(printf ",%s" "${QUERIES[@]}")
export QUERIES_STRING

cd "$DBGEN_DIR" || exit

get_scales
for i in "${SF[@]}"
do
    ./dbgen -f -s "$i"
    mkdir -p "$DBGEN_DATA_DIR/$i/"
    mv "$DBGEN_DIR"/*.tbl  "$DBGEN_DATA_DIR/$i/"
done

cd - || exit

declare -A tables
tables["customer"]=8
tables["lineitem"]=16
tables["nation"]=4
tables["orders"]=9
tables["partsupp"]=5
tables["part"]=9
tables["region"]=3
tables["supplier"]=7

for i in "${SF[@]}"
do
    for j in "${!tables[@]}"
    do
        mkdir -p "$LA_DATA_DIR/${i}/${j}_cols/" || exit
        for (( k=1; k<=${tables[$j]}; ++k ))
        do
            echo "Scale factor: ${i}; table ${j}; column ${k}"
            gawk -F '|' -v c=$k '{ print $c }' "$DBGEN_DATA_DIR/$i/${j}.tbl" > "$LA_DATA_DIR/$i/${j}_cols/${j}_${k}.tbl"
        done
    done
done


cd "$REPO_DIR/la" || exit
make "-j${MAKE_NUM_THREADS}" || exit
cd - || exit

sh "$REPO_DIR/mysql/mysql_create_test_environment.sh"
sh "$REPO_DIR/postgresql/postgres_create_test_environment.sh"

sh "$REPO_DIR/la/la_test_tpch_queries.sh"
sh "$REPO_DIR/mysql/mysql_test_tpch_queries.sh"
sh "$REPO_DIR/postgresql/postgres_test_tpch_queries.sh"
