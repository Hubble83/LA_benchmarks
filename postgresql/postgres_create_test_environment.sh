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

# Create dab«tabase laolap
"$PG_DIR/postgresql/bin/createdb" laolap -O root -U root -w -e

# Config PostgreSQL
"$PG_DIR/postgresql/bin/psql" laolap -U laolap -f <<< "ALTER SYSTEM SET shared_buffers TO '192GB';" # 3/4 256
"$PG_DIR/postgresql/bin/psql" laolap -U laolap -f <<< "ALTER SYSTEM SET effective_cache_size TO '224GB';" # 256 - 32(offset)
