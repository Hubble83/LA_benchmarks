#!/bin/bash

# Download Boost
cd "$BOOST_DIR" || exit
wget --no-check-certificate https://sourceforge.net/projects/boost/files/boost/1.59.0/boost_1_59_0.tar.gz/download
tar -zxvf boost_1_59_0.tar.gz
rm boost_1_59_0.tar.gz

# Download and install Bison
mkdir -p "$BISON_DIR/src"
cd "$BISON_DIR/src" || exit
wget --no-check-certificate http://mirrors.up.pt/pub/gnu/bison/bison-3.0.tar.gz
tar -zxvf bison-3.0.tar.gz
rm bison-3.0.tar.gz
cd bison-3.0/ || exit
./configure --prefix "$BISON_DIR/bison"
make "-j${MAKE_NUM_THREADS}"
make install

# Download and install MySQL
mkdir -p "$MYSQL_DIR/src"
mkdir -p "$MYSQL_DIR/mysql/data"
mkdir -p "$MYSQL_DIR/mysql/etc"
mkdir -p "$MYSQL_DIR/mysql/log"
mkdir -p "$MYSQL_DIR/mysql/tmp"
cd "$MYSQL_DIR/src" || exit
git clone https://github.com/mysql/mysql-server.git
cd mysql-server || exit
cmake -D MYSQL_DATADIR="$MYSQL_DIR/mysql/data/" -D SYSCONFIG="$MYSQL_DIR/mysql/etc/" -D CMAKE_INSTALL_PREFIX="$MYSQL_DIR/mysql/" -D WITH_BOOST="$BOOST_DIR/boost_1_59_0/" .
make "-j${MAKE_NUM_THREADS}"
make install

# Download dstat
cd "$LIB_DIR" || exit
git clone https://github.com/dagwieers/dstat.git

# Create MySQL configuration file (my.cnf)
{
	echo -e "[client]"
	echo -e "port = 3306"
	echo -e "socket = $MYSQL_DIR/mysql/mysql.sock"
	echo -e ""
	echo -e "[mysql]"
	echo -e "no_auto_rehash"
	echo -e "default_character_set = utf8"
	echo -e ""
	echo -e "[mysqld_safe]"
	echo -e "user = mysql"
	echo -e "log-error = $MYSQL_DIR/mysql/mysqld.log"
	echo -e ""
	echo -e "[mysqld]"
	echo -e "port = 3306"
	echo -e "socket = $MYSQL_DIR/mysql/mysql.sock"
	echo -e "basedir = $MYSQL_DIR/mysql"
	echo -e "datadir = $MYSQL_DIR/mysql/data"
	echo -e "tmpdir = $MYSQL_DIR/mysql/tmp"
	echo -e "default_storage_engine = MEMORY"
	echo -e "disable_partition_engine_check = true"
	echo -e "pid-file = $MYSQL_DIR/mysql/mysql.pid"
	echo -e "secure-file-priv = \"\""
	echo -e "max_heap_table_size=$((RAM_SIZE * 98 / 100))G"
	echo -e "skip_log_bin"
	echo -e "query_cache_type = 0"
	echo -e "query_cache_size = 0"
	echo -e "performance_schema = off"
	echo -e ""
} > "$MY_CNF"

# Initialize MySQL
TPW=$("$MYSQL_DIR/mysql/bin/mysqld" --defaults-file="$MY_CNF" --initialize |& grep localhost | grep -oE "[^ ]+$")

# Start the server in background:
"$MYSQL_DIR/mysql/bin/mysqld_safe" --defaults-file="$MY_CNF" &

# Wait server started
sleep 5

# Change the deafult password
"$MYSQL_DIR/mysql/bin/mysql" --defaults-file="$MY_CNF" --connect-expired-password -u "root" "-p${TPW}" -e "ALTER USER 'root'@'localhost' IDENTIFIED BY 'root';"

# Create the database
"$MYSQL_DIR/mysql/bin/mysql" --defaults-file="$MY_CNF" -u "root" "-proot" -e "CREATE DATABASE laolap;"

# Create all tables
get_scales
for i in "${SF[@]}"
do
	{
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
	} > "mysql_tmp_create_tables.sql"

	"$MYSQL_DIR/mysql/bin/mysql" --defaults-file="$MY_CNF" -u "root" "-proot" laolap < "mysql_tmp_create_tables.sql"
done

rm "mysql_tmp_create_tables.sql"

# Shutdown the server
"$MYSQL_DIR/mysql/bin/mysqladmin" --defaults-file="$MY_CNF" -u "root" "-proot" shutdown
