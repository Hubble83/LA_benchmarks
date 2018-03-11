#!/bin/bash

# Download Boost
cd "$BOOST_DIR" || exit
wget --no-check-certificate https://sourceforge.net/projects/boost/files/boost/1.59.0/boost_1_59_0.tar.gz
tar -zxvf boost_1_59_0.tar.gz
rm boost_1_59_0.tar.gz
