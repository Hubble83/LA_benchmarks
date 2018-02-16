#!/bin/bash

#PBS -N MYSQL_BENCHMARK
#PBS -l walltime=24:00:00
#PBS -l nodes=compute-781-1:ppn=64
#PBS -m abe
#PBS -M a71874@alunos.uminho.pt

# Specify libs directory
export LIB_DIR=$HOME/new_libs

# Specify lib subdirectories
export MYSQL_DIR=$LIB_DIR/mysql
export DSTAT_DIR=$LIB_DIR/dstat

# Specify MySQL configuration file (my.cnf)
export MY_CNF=$MYSQL_DIR/mysql/my.cnf

# Specify data dir
export DATA_DIR=/share/jade/laolap_data

################################################################################

mkdir -p "results_mysql"

for q in 3 4 6 11 12 14
do
echo "dataset,k-best,average,std_dev" > "results_mysql/query_${q}_time.csv"
echo "dataset,k-best,average,std_dev" > "results_mysql/query_${q}_memory.csv"
done

for i in 1 2 4 8 16 32 64 128
do
# Start the server in background:
"$MYSQL_DIR/mysql/bin/mysqld_safe" --defaults-file="$MY_CNF" &>> benchmark.log &

# Wait server started
sleep 5

# Create data load script
{
echo -e "LOAD DATA INFILE '$DATA_DIR/$i/part.tbl' INTO TABLE part_$i COLUMNS TERMINATED BY '|';"
echo -e "LOAD DATA INFILE '$DATA_DIR/$i/region.tbl' INTO TABLE region_$i COLUMNS TERMINATED BY '|';"
echo -e "LOAD DATA INFILE '$DATA_DIR/$i/nation.tbl' INTO TABLE nation_$i COLUMNS TERMINATED BY '|';"
echo -e "LOAD DATA INFILE '$DATA_DIR/$i/supplier.tbl' INTO TABLE supplier_$i COLUMNS TERMINATED BY '|';"
echo -e "LOAD DATA INFILE '$DATA_DIR/$i/partsupp.tbl' INTO TABLE partsupp_$i COLUMNS TERMINATED BY '|';"
echo -e "LOAD DATA INFILE '$DATA_DIR/$i/customer.tbl' INTO TABLE customer_$i COLUMNS TERMINATED BY '|';"
echo -e "LOAD DATA INFILE '$DATA_DIR/$i/orders.tbl' INTO TABLE orders_$i COLUMNS TERMINATED BY '|';"
echo -e "LOAD DATA INFILE '$DATA_DIR/$i/lineitem.tbl' INTO TABLE lineitem_$i COLUMNS TERMINATED BY '|';"
} > "$HOME/mysql_tmp_population_queries.sql"

# Load data
"$MYSQL_DIR/mysql/bin/mysql" --defaults-file="$MY_CNF" -u "root" "-proot" laolap < "$HOME/mysql_tmp_population_queries.sql" &>> benchmark.log

# Remove created sript
rm "$HOME/mysql_tmp_population_queries.sql" &>> benchmark.log

# Test the current dataset
python $HOME/VLDB_benchmarks/mysql/mysql.py "$i" &>> benchmark.log

# Shutdown the server to free memory
"$MYSQL_DIR/mysql/bin/mysqladmin" --defaults-file="$MY_CNF" -u "root" "-proot" shutdown &>> benchmark.log
sleep 5

done
