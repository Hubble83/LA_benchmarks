#!/bin/bash -x

# Download and install OpenSSL
mkdir -p "$OPENSSL_DIR/src"
cd "$OPENSSL_DIR/src" || exit
git clone https://github.com/openssl/openssl.git
mkdir -p "$OPENSSL_DIR/src/openssl/build"
cd "$OPENSSL_DIR/src/openssl/build" || exit
../config --prefix="$OPENSSL_DIR/openssl" --openssldir="$OPENSSL_DIR/openssl" shared zlib
make "-j${MAKE_NUM_THREADS}"
make install
