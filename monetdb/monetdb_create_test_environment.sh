#!/bin/bash -x

# Download and install MonetDB
mkdir -p "$MONET_DIR/src"
cd "$MONET_DIR/src" || exit
wget --no-check-certificate https://www.monetdb.org/downloads/sources/Mar2018/MonetDB-11.29.3.tar.bz2
tar -xvjf MonetDB-11.29.3.tar.bz2
rm MonetDB-11.29.3.tar.bz2
mkdir -p "$MONET_DIR/src/MonetDB-11.29.3/build"
cd "$MONET_DIR/src/MonetDB-11.29.3/build" || exit

export PATH="$OPENSSL_DIR/openssl/include:$PATH"
export PATH="$OPENSSL_DIR/openssl/bin:$PATH"
export PATH="$OPENSSL_DIR/openssl/lib:$PATH"

export PKG_CONFIG_PATH="$PKG_CONFIG_PATH:$PATH"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PATH"

export openssl_CFLAGS="-I$OPENSSL_DIR/openssl/include"
export openssl_LIBS="-L$OPENSSL_DIR/openssl/lib -lcrypto -lssl"

../configure --prefix="$MONET_DIR/monetdb" \
             --enable-debug=no \
             --enable-assert=no \
             --enable-optimize=yes

make "-j${MAKE_NUM_THREADS}"
make install


# Initialize MonetDB
"$MONET_DIR/monetdb/bin/monetdbd" create my-dbfarm || exit
"$MONET_DIR/monetdb/bin/monetdbd" start my-dbfarm

# Create database
"$MONET_DIR/monetdb/bin/monetdb" create laolap
"$MONET_DIR/monetdb/bin/monetdb" release laolap

{
    echo -e "user=monetdb"
    echo -e "password=monetdb"
    echo -e "language=sql"
} > ~/.monetdb

get_scales
echo "${SF[@]}"

# Create all tables
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

echo -e "COPY INTO part_$i FROM '$DBGEN_DATA_DIR/$i/part.tbl' USING DELIMITERS '|', '|\\\n';"
echo -e "COPY INTO region_$i FROM '$DBGEN_DATA_DIR/$i/region.tbl' USING DELIMITERS '|', '|\\\n';"
echo -e "COPY INTO nation_$i FROM '$DBGEN_DATA_DIR/$i/nation.tbl' USING DELIMITERS '|', '|\\\n';"
echo -e "COPY INTO supplier_$i FROM '$DBGEN_DATA_DIR/$i/supplier.tbl' USING DELIMITERS '|', '|\\\n';"
echo -e "COPY INTO partsupp_$i FROM '$DBGEN_DATA_DIR/$i/partsupp.tbl' USING DELIMITERS '|', '|\\\n';"
echo -e "COPY INTO customer_$i FROM '$DBGEN_DATA_DIR/$i/customer.tbl' USING DELIMITERS '|', '|\\\n';"
echo -e "COPY INTO orders_$i FROM '$DBGEN_DATA_DIR/$i/orders.tbl' USING DELIMITERS '|', '|\\\n';"
echo -e "COPY INTO lineitem_$i FROM '$DBGEN_DATA_DIR/$i/lineitem.tbl' USING DELIMITERS '|', '|\\\n';"
} > "monetdb_tmp_create_tables.sql"

cat "monetdb_tmp_create_tables.sql"

# Start MonetDB server
"$MONET_DIR/monetdb/bin/mclient" -d laolap "monetdb_tmp_create_tables.sql"
done

rm "monetdb_tmp_create_tables.sql"

# Shutdown the server
"$MONET_DIR/monetdb/bin/monetdbd" stop my-dbfarm
