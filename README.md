# VLDB_benchmarks

This repository contains the necessary scripts to recreate all the benchmarks presented in the paper.

_Note: Edit predifined variables in the top of each script to fit the ones in your system_

## MySQL

 **mysql_create_test_environment.sh** - used to install and configure MySQL, create tables and generate TPC-H data.

 **mysql_test_tpch_queries.sh** - used to test all mysql queries (expects a valid test environment to be created)
 + Estimated time: 6 hours

## PostgreSQL

 **postgres_create_test_environment.sh** - used to install and configure PostgreSQL, create tables and load data.
 + Estimated time: 48 hours

## Linear Algebra approach
