#!/bin/bash

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
