#!/bin/bash

#PBS -N INSTALL_POSTGRESQL
#PBS -l walltime=03:00:00
#PBS -l nodes=compute-781-1:ppn=64
#PBS -m abe
#PBS -M a71874@alunos.uminho.pt

# Specify libs directory
export LIB_DIR=$HOME/new_libs

# Specify number of threads used by make
export MAKE_NUM_THREADS=64

# Specify total RAM in GB
export RAM_SIZE=256

# Specify lib subdirectories
export READLINE_DIR=$LIB_DIR/readline
export PG_DIR=$LIB_DIR/postgresql

# Specify data dir
export DATA_DIR=/share/jade/laolap_data

################################################################################

mkdir -p "$PG_DIR" "$READLINE_DIR"

# Download and install readline
mkdir -p "$READLINE_DIR/src"
cd "$READLINE_DIR/src" || return
wget --no-check-certificate https://ftp.gnu.org/gnu/readline/readline-7.0.tar.gz
tar -zxvf readline-7.0.tar.gz
rm readline-7.0.tar.gz
mkdir -p "$READLINE_DIR/src/readline-7.0/build"
cd "$READLINE_DIR/src/readline-7.0/build" || return
../configure --prefix "$READLINE_DIR/readline"
make -j$MAKE_NUM_THREADS
make install

# Download and install PostgreSQL
mkdir -p "$PG_DIR/src"
cd "$PG_DIR/src" || return
wget --no-check-certificate https://ftp.postgresql.org/pub/source/v10.2/postgresql-10.2.tar.gz
tar -zxvf postgresql-10.2.tar.gz
rm postgresql-10.2.tar.gz
mkdir -p "$PG_DIR/postgresql/data"
mkdir -p "$PG_DIR/postgresql/pgdata"
mkdir -p "$PG_DIR/src/postgresql-10.2/build"
cd "$PG_DIR/src/postgresql-10.2/build" || return
export LD_LIBRARY_PATH="$READLINE_DIR/readline/lib:$LD_LIBRARY_PATH"
../configure --prefix "$PG_DIR/postgresql" \
	--datadir="$PG_DIR/postgresql/data" \
	--localedir="$PG_DIR/postgresql/data/locale" \
	--with-includes="$READLINE_DIR/readline/include" \
	--with-libraries="$READLINE_DIR/readline/lib:$LD_LIBRARY_PATH"
make -j$MAKE_NUM_THREADS
make install

# Initialize PostgreSQL
"$PG_DIR/postgresql/bin/initdb" -D "$PG_DIR/postgresql/pgdata"

# Start PostgreSQL server
"$PG_DIR/postgresql/bin/pg_ctl" -D "$PG_DIR/postgresql/pgdata" -l "$PG_DIR/postgresql/postgresql.log" start

# Wait server started
sleep 5

# Create database laolap
"$PG_DIR/postgresql/bin/createdb" laolap -O laolap -U laolap -w -e

# Config PostgreSQL
"$PG_DIR/postgresql/bin/psql" laolap -U laolap -c "ALTER SYSTEM SET shared_buffers TO '$((RAM_SIZE * 3 / 4))GB';"
"$PG_DIR/postgresql/bin/psql" laolap -U laolap -c "ALTER SYSTEM SET effective_cache_size TO '$((RAM_SIZE * 7 / 8))GB';"

# restart server to apply configs
"$PG_DIR/postgresql/bin/pg_ctl" -D "$PG_DIR/postgresql/pgdata" -l "$PG_DIR/postgresql/postgresql.log" restart

# Wait server started
sleep 5

# Create all tables
for i in 1 2 4 8 16 32 64 128
do {
echo -e "CREATE TABLE part_${i} ("
echo -e "        p_partkey       INTEGER NOT NULL,"
echo -e "        p_name          VARCHAR(55) NOT NULL,"
echo -e "        p_mfgr          CHAR(25) NOT NULL,"
echo -e "        p_brand         CHAR(10) NOT NULL,"
echo -e "        p_type          VARCHAR(25) NOT NULL,"
echo -e "        p_size          INTEGER NOT NULL,"
echo -e "        p_container     CHAR(10) NOT NULL,"
echo -e "        p_retailprice   DECIMAL(12,2) NOT NULL,"
echo -e "        p_comment       VARCHAR(23) NOT NULL,"
echo -e "        PRIMARY KEY (p_partkey)"
echo -e ");"

echo -e "CREATE TABLE region_${i} ("
echo -e "        r_regionkey     INTEGER NOT NULL,"
echo -e "        r_name          CHAR(25) NOT NULL,"
echo -e "        r_comment       VARCHAR(152) NOT NULL,"
echo -e "        PRIMARY KEY (r_regionkey)"
echo -e ");"

echo -e "CREATE TABLE nation_${i} ("
echo -e "        n_nationkey     INTEGER NOT NULL,"
echo -e "        n_name          CHAR(25) NOT NULL,"
echo -e "        n_regionkey     INTEGER NOT NULL,"
echo -e "        n_comment       VARCHAR(152) NOT NULL,"
echo -e "        PRIMARY KEY (n_nationkey),"
echo -e "        CONSTRAINT fk_n_regionkey FOREIGN KEY (n_regionkey) REFERENCES region_${i} (r_regionkey)"
echo -e ");"
echo -e "CREATE INDEX idx_${i}_n_regionkey ON nation_${i} (n_regionkey);"

echo -e "CREATE TABLE supplier_${i} ("
echo -e "        s_suppkey       INTEGER NOT NULL,"
echo -e "        s_name          CHAR(25) NOT NULL,"
echo -e "        s_address       VARCHAR(40) NOT NULL,"
echo -e "        s_nationkey     INTEGER NOT NULL,"
echo -e "        s_phone         CHAR(15) NOT NULL,"
echo -e "        s_acctbal       DECIMAL(12,2) NOT NULL,"
echo -e "        s_comment       VARCHAR(101) NOT NULL,"
echo -e "        PRIMARY KEY (s_suppkey),"
echo -e "        CONSTRAINT fk_s_nationkey FOREIGN KEY (s_nationkey) REFERENCES nation_${i} (n_nationkey)"
echo -e ");"
echo -e "CREATE INDEX idx_${i}_s_nationkey ON supplier_${i} (s_nationkey);"

echo -e "CREATE TABLE partsupp_${i} ("
echo -e "        ps_partkey      INTEGER NOT NULL,"
echo -e "        ps_suppkey      INTEGER NOT NULL,"
echo -e "        ps_availqty     INTEGER NOT NULL,"
echo -e "        ps_supplycost   DECIMAL(12,2) NOT NULL,"
echo -e "        ps_comment      VARCHAR(199) NOT NULL,"
echo -e "        PRIMARY KEY (ps_partkey, ps_suppkey),"
echo -e "        CONSTRAINT fk_ps_partkey FOREIGN KEY (ps_partkey) REFERENCES part_${i} (p_partkey),"
echo -e "        CONSTRAINT fk_ps_suppkey FOREIGN KEY (ps_suppkey) REFERENCES supplier_${i} (s_suppkey)"
echo -e ");"
echo -e "CREATE INDEX idx_${i}_ps_partkey ON partsupp_${i} (ps_partkey);"
echo -e "CREATE INDEX idx_${i}_ps_suppkey ON partsupp_${i} (ps_suppkey);"

echo -e "CREATE TABLE customer_${i} ("
echo -e "        c_custkey       INTEGER NOT NULL,"
echo -e "        c_name          VARCHAR(25) NOT NULL,"
echo -e "        c_address       VARCHAR(40) NOT NULL,"
echo -e "        c_nationkey     INTEGER NOT NULL,"
echo -e "        c_phone         CHAR(15) NOT NULL,"
echo -e "        c_acctbal       DECIMAL(12,2) NOT NULL,"
echo -e "        c_mktsegment    CHAR(10) NOT NULL,"
echo -e "        c_comment       VARCHAR(117) NOT NULL,"
echo -e "        PRIMARY KEY (c_custkey),"
echo -e "        CONSTRAINT fk_c_nationkey FOREIGN KEY (c_nationkey) REFERENCES nation_${i} (n_nationkey)"
echo -e ");"
echo -e "CREATE INDEX idx_${i}_c_nationkey ON customer_${i} (c_nationkey);"

echo -e "CREATE TABLE orders_${i} ("
echo -e "        o_orderkey      INTEGER NOT NULL,"
echo -e "        o_custkey       INTEGER NOT NULL,"
echo -e "        o_orderstatus   CHAR(1) NOT NULL,"
echo -e "        o_totalprice    DECIMAL(12,2) NOT NULL,"
echo -e "        o_orderdate     DATE NOT NULL,"
echo -e "        o_orderpriority CHAR(15) NOT NULL,"
echo -e "        o_clerk         CHAR(15) NOT NULL,"
echo -e "        o_shippriority  INTEGER NOT NULL,"
echo -e "        o_comment       VARCHAR(79) NOT NULL,"
echo -e "        PRIMARY KEY (o_orderkey),"
echo -e "        CONSTRAINT fk_o_custkey FOREIGN KEY (o_custkey) REFERENCES customer_${i} (c_custkey)"
echo -e ");"
echo -e "CREATE INDEX idx_${i}_o_custkey ON orders_${i} (o_custkey);"

echo -e "CREATE TABLE lineitem_${i} ("
echo -e "        l_orderkey      INTEGER NOT NULL,"
echo -e "        l_partkey       INTEGER NOT NULL,"
echo -e "        l_suppkey       INTEGER NOT NULL,"
echo -e "        l_linenumber    INTEGER NOT NULL,"
echo -e "        l_quantity      DECIMAL(12,2) NOT NULL,"
echo -e "        l_extendedprice DECIMAL(12,2) NOT NULL,"
echo -e "        l_discount      DECIMAL(12,2) NOT NULL,"
echo -e "        l_tax           DECIMAL(12,2) NOT NULL,"
echo -e "        l_returnflag    CHAR(1) NOT NULL,"
echo -e "        l_linestatus    CHAR(1) NOT NULL,"
echo -e "        l_shipdate      DATE NOT NULL,"
echo -e "        l_commitdate    DATE NOT NULL,"
echo -e "        l_receiptdate   DATE NOT NULL,"
echo -e "        l_shipinstruct  CHAR(25) NOT NULL,"
echo -e "        l_shipmode      CHAR(10) NOT NULL,"
echo -e "        l_comment       VARCHAR(44) NOT NULL,"
echo -e "        PRIMARY KEY (l_orderkey, l_linenumber),"
echo -e "        CONSTRAINT fk_l_orderkey FOREIGN KEY (l_orderkey) REFERENCES orders_${i} (o_orderkey),"
echo -e "        CONSTRAINT fk_l_partkey FOREIGN KEY (l_partkey) REFERENCES part_${i} (p_partkey),"
echo -e "        CONSTRAINT fk_l_suppkey FOREIGN KEY (l_suppkey) REFERENCES supplier_${i} (s_suppkey)"
echo -e ");"
echo -e "CREATE INDEX idx_${i}_l_orderkey ON lineitem_${i} (l_orderkey);"
echo -e "CREATE INDEX idx_${i}_l_partkey ON lineitem_${i} (l_partkey);"
echo -e "CREATE INDEX idx_${i}_l_suppkey ON lineitem_${i} (l_suppkey);"

echo -e "COPY part_$i FROM PROGRAM 'sed ''s/.$//'' $DATA_DIR/$i/part.tbl' WITH DELIMITER AS '|';"
echo -e "COPY region_$i FROM PROGRAM 'sed ''s/.$//'' $DATA_DIR/$i/region.tbl' WITH DELIMITER AS '|';"
echo -e "COPY nation_$i FROM PROGRAM 'sed ''s/.$//'' $DATA_DIR/$i/nation.tbl' WITH DELIMITER AS '|';"
echo -e "COPY supplier_$i FROM PROGRAM 'sed ''s/.$//'' $DATA_DIR/$i/supplier.tbl' WITH DELIMITER AS '|';"
echo -e "COPY partsupp_$i FROM PROGRAM 'sed ''s/.$//'' $DATA_DIR/$i/partsupp.tbl' WITH DELIMITER AS '|';"
echo -e "COPY customer_$i FROM PROGRAM 'sed ''s/.$//'' $DATA_DIR/$i/customer.tbl' WITH DELIMITER AS '|';"
echo -e "COPY orders_$i FROM PROGRAM 'sed ''s/.$//'' $DATA_DIR/$i/orders.tbl' WITH DELIMITER AS '|';"
echo -e "COPY lineitem_$i FROM PROGRAM 'sed ''s/.$//'' $DATA_DIR/$i/lineitem.tbl' WITH DELIMITER AS '|';"
} > "postgres_tmp_create_tables.sql"

"$PG_DIR/postgresql/bin/psql" laolap -U laolap -f "postgres_tmp_create_tables.sql"
done

rm "postgres_tmp_create_tables.sql"

# Shutdown the server
"$PG_DIR/postgresql/bin/pg_ctl" -D "$PG_DIR/postgresql/pgdata" -l "$PG_DIR/postgresql/postgresql.log" stop
