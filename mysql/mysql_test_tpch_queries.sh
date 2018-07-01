#!/bin/bash -x

mkdir -p "results_mysql"

get_queries
for q in "${QUERIES[@]}"
do
    echo "dataset,k-best,average,std_dev" > "results_mysql/query_${q}_time.csv"
    echo "dataset,k-best,average,std_dev" > "results_mysql/query_${q}_memory.csv"
done

get_scales
for i in "${SF[@]}"
do
    # Start the server in background:
    "$MYSQL_DIR/mysql/bin/mysqld_safe" --defaults-file="$MY_CNF" &

    # Wait server started
    sleep 5

    # Create data load script
    {
    echo -e "LOAD DATA INFILE '$DBGEN_DATA_DIR/$i/part.tbl' INTO TABLE part_$i COLUMNS TERMINATED BY '|';"
    echo -e "LOAD DATA INFILE '$DBGEN_DATA_DIR/$i/region.tbl' INTO TABLE region_$i COLUMNS TERMINATED BY '|';"
    echo -e "LOAD DATA INFILE '$DBGEN_DATA_DIR/$i/nation.tbl' INTO TABLE nation_$i COLUMNS TERMINATED BY '|';"
    echo -e "LOAD DATA INFILE '$DBGEN_DATA_DIR/$i/supplier.tbl' INTO TABLE supplier_$i COLUMNS TERMINATED BY '|';"
    echo -e "LOAD DATA INFILE '$DBGEN_DATA_DIR/$i/partsupp.tbl' INTO TABLE partsupp_$i COLUMNS TERMINATED BY '|';"
    echo -e "LOAD DATA INFILE '$DBGEN_DATA_DIR/$i/customer.tbl' INTO TABLE customer_$i COLUMNS TERMINATED BY '|';"
    echo -e "LOAD DATA INFILE '$DBGEN_DATA_DIR/$i/orders.tbl' INTO TABLE orders_$i COLUMNS TERMINATED BY '|';"
    echo -e "LOAD DATA INFILE '$DBGEN_DATA_DIR/$i/lineitem.tbl' INTO TABLE lineitem_$i COLUMNS TERMINATED BY '|';"
    } > mysql_tmp_population_queries.sql

    # Load data
    "$MYSQL_DIR/mysql/bin/mysql" --defaults-file="$MY_CNF" -u "root" "-proot" laolap < mysql_tmp_population_queries.sql

    # Remove created sript
    rm mysql_tmp_population_queries.sql

    # Test the current dataset
    python "$REPO_DIR/mysql/mysql.py" "$i"

    # Shutdown the server to free memory
    "$MYSQL_DIR/mysql/bin/mysqladmin" --defaults-file="$MY_CNF" -u "root" "-proot" shutdown
    sleep 5

done
