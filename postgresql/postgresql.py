#!/usr/bin/env python

import sys, subprocess, os, time
from datetime import datetime

def kbest (k, values):
  error=(1,-1)
  values.sort()
  for i in range(len(values)-k):
    maximum = values[i+k-1]
    minimum = values[i]
    if maximum==0:
      e=0
    else:
      e = (maximum - minimum) / float(maximum)
    if e < 0.05:
      return sum(values[i:i+k]) / float(k)
    if e < error[0]:
      error=(e,i)
  if error[1] != -1:
    return sum(values[error[1]:error[1]+k]) / float(k)
  return -1

def avg(values):
  return sum(values) / float(len(values))

def sdev(mean, values):
  return avg(map(lambda x: (x-mean)**2, values))**(0.5)

def main(argv):

  data_size = argv[1]
  query = str(argv[2])

  PG_DIR=os.environ["PG_DIR"]
  DSTAT_DIR=os.environ["DSTAT_DIR"]

  with open("results_postgres/query_{0}_time.csv".format(query), "a") as csv_time:
    csv_time.write(data_size)
  with open("results_postgres/query_{0}_memory.csv".format(query), "a") as csv_memory:
    csv_memory.write(data_size)


  exec_time = []
  memory = []

  # Warm up cache
  for i in range(2):
    execution = subprocess.Popen(
      [
        "{0}/postgresql/bin/psql".format(PG_DIR),
        "laolap", "-U", "laolap", "-c", queries[query].format(data_size)
      ],
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT
    )
    execution.wait()

  for i in range(10):

    dstat = subprocess.Popen(
      [DSTAT_DIR+"/dstat", "-m", "--noheaders", "--noupdate", "--nocolor", "--output", "postgres_memory_usage.csv"],
      stdout=subprocess.PIPE
    )
    time.sleep(2)

    with open("postgres_current_query.sql", "w") as curr_query:
      curr_query.write(queries[query].format(data_size))

    # run query
    execution = subprocess.Popen(
      [
        "{0}/postgresql/bin/psql".format(PG_DIR),
        "laolap", "-U", "laolap", "-f", "postgres_current_query.sql"
      ],
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT
    )
    execution.wait()

    os.remove("postgres_current_query.sql")
    # get query results_postgres
    output, _ = execution.communicate()
    output_line_list = output.splitlines()

    # stop profiler
    time.sleep(2)
    dstat.terminate()

    # get profiler results_postgres
    mem_out_list=[]
    with open("postgres_memory_usage.csv", "r") as mem_info:
      mem_out_list = mem_info.readlines()[7:-1]
    os.remove("postgres_memory_usage.csv")

    # calculate max memory usage
    memory.append(
      max(map( lambda x: float(x.split(',')[0]), mem_out_list ))
    )

    # calculate execution time
    t_str = filter(lambda x: "Time:" in x, output_line_list)[-1].split()[1]
    t_acc = float(t_str)/1000.0
    exec_time.append( t_acc )


  # aggregate data from multiple runs storing the k-best, average and standard deviation
  with open("results_postgres/query_{0}_time.csv".format(query), "a") as csv_time:
    kb = round( kbest(3,exec_time), 6)
    av = round( avg(exec_time), 6)
    sd = round( sdev(av,exec_time), 6)
    csv_time.write( ",{0},{1},{2}\n".format(kb, av, sd) )
  with open("results_postgres/query_{0}_memory.csv".format(query), "a") as csv_memory:
    kb = round( kbest(3,memory), 6)
    av = round( avg(memory), 6)
    sd = round( sdev(av,memory), 6)
    csv_memory.write( ",{0},{1},{2}\n".format(kb, av, sd) )


queries = {
  "3": ("\\timing on\n"
        "set max_parallel_workers_per_gather=0;\n"
        "set max_parallel_workers=0;\n"
        "select "
        "l_orderkey, "
        "sum(l_extendedprice * (1 - l_discount)) as revenue, "
        "o_orderdate, "
        "o_shippriority "
        "from "
        "customer_{0}, "
        "orders_{0}, "
        "lineitem_{0} "
        "where "
        "c_mktsegment = 'MACHINERY' "
        "and c_custkey = o_custkey "
        "and l_orderkey = o_orderkey "
        "and o_orderdate < date '1995-03-10' "
        "and l_shipdate > date '1995-03-10' "
        "group by "
        "l_orderkey, "
        "o_orderdate, "
        "o_shippriority "
        "order by "
        "revenue desc, "
        "o_orderdate;"
        "\n\\timing off"
  ),

  "4": ("\\timing on\n"
        "set max_parallel_workers_per_gather=0;\n"
        "set max_parallel_workers=0;\n"
        "select "
        "o_orderpriority, "
        "count(*) as order_count "
        "from "
        "orders_{0} "
        "where "
        "o_orderdate >= date '1993-07-01' "
        "and o_orderdate < date '1993-10-01' "
        "and exists ( "
        "select * "
        "from "
        "lineitem_{0} "
        "where "
        "l_orderkey = o_orderkey "
        "and l_commitdate < l_receiptdate) "
        "group by "
        "o_orderpriority;"
        "\n\\timing off"
  ),

  "6": ("\\timing on\n"
        "set max_parallel_workers_per_gather=0;\n"
        "set max_parallel_workers=0;\n"
        "select "
        "sum(l_extendedprice * l_discount) as revenue "
        "from "
        "lineitem_{0} "
        "where "
        "l_shipdate >= date '1995-03-10' "
        "and l_shipdate < date '1996-03-10' "
        "and l_discount between 0.04 and 0.06 "
        "and l_quantity < 3;"
        "\n\\timing off"
  ),

  "11": ("\\timing on\n"
        "set max_parallel_workers_per_gather=0;\n"
        "set max_parallel_workers=0;\n"
        "select "
        "ps_partkey, "
        "sum(ps_supplycost * ps_availqty) as value "
        "from "
        "partsupp_{0}, "
        "supplier_{0}, "
        "nation_{0} "
        "where "
        "ps_suppkey = s_suppkey "
        "and s_nationkey = n_nationkey "
        "and n_name = 'GERMANY' "
        "group by "
        "ps_partkey having "
        "sum(ps_supplycost * ps_availqty) > ( "
        "select "
        "sum(ps_supplycost * ps_availqty) * 0.0001 "
        "from "
        "partsupp_{0}, "
        "supplier_{0}, "
        "nation_{0} "
        "where "
        "ps_suppkey = s_suppkey "
        "and s_nationkey = n_nationkey "
        "and n_name = 'GERMANY');"
        "\n\\timing off"
  ),

  "12": ("\\timing on\n"
        "set max_parallel_workers_per_gather=0;\n"
        "set max_parallel_workers=0;\n"
        "select "
        "l_shipmode, "
        "sum(case "
        "when o_orderpriority = '1-URGENT' "
        "or o_orderpriority = '2-HIGH' "
        "then 1 "
        "else 0 "
        "end) as high_line_count, "
        "sum(case "
        "when o_orderpriority <> '1-URGENT' "
        "and o_orderpriority <> '2-HIGH' "
        "then 1 "
        "else 0 "
        "end) as low_line_count "
        "from "
        "orders_{0}, "
        "lineitem_{0} "
        "where "
        "o_orderkey = l_orderkey "
        "and l_shipmode in ('MAIL', 'SHIP') "
        "and l_commitdate < l_receiptdate "
        "and l_shipdate < l_commitdate "
        "and l_receiptdate >= date '1994-01-01' "
        "and l_receiptdate < date '1995-01-01' "
        "group by "
        "l_shipmode;"
        "\n\\timing off"
  ),

  "14": ("\\timing on\n"
        "set max_parallel_workers_per_gather=0;\n"
        "set max_parallel_workers=0;\n"
        "select "
        "100.00 * sum(case "
        "when p_type like 'PROMO%' "
        "then l_extendedprice * (1 - l_discount) "
        "else 0 "
        "end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue "
        "from "
        "lineitem_{0}, "
        "part_{0} "
        "where "
        "l_partkey = p_partkey "
        "and l_shipdate >= date '1995-09-01' "
        "and l_shipdate < date '1995-10-01';"
        "\n\\timing off"

  )
}

if __name__ == "__main__":
  main(sys.argv)
