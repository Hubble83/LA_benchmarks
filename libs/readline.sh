#!/bin/bash

# Download and install readline
mkdir -p "$READLINE_DIR/src"
cd "$READLINE_DIR/src" || exit
wget --no-check-certificate https://ftp.gnu.org/gnu/readline/readline-7.0.tar.gz
tar -zxvf readline-7.0.tar.gz
rm readline-7.0.tar.gz
mkdir -p "$READLINE_DIR/src/readline-7.0/build"
cd "$READLINE_DIR/src/readline-7.0/build" || exit
../configure --prefix "$READLINE_DIR/readline"
make "-j${MAKE_NUM_THREADS}"
make install
export PATH="$PATH:$READLINE_DIR/readline:$READLINE_DIR/readline/bin:$READLINE_DIR/readline/lib"
