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

  MYSQL_DIR=os.environ["MYSQL_DIR"]
  MY_CNF=os.environ["MY_CNF"]
  DSTAT_DIR=os.environ["DSTAT_DIR"]

  for q in queries:
    with open("results_mysql/query_{0}_time.csv".format(q), "a") as csv_time:
      csv_time.write(data_size)
    with open("results_mysql/query_{0}_memory.csv".format(q), "a") as csv_memory:
      csv_memory.write(data_size)

  for num, query in queries.iteritems():

    exec_time = []
    memory = []

    for i in range(10):

      dstat = subprocess.Popen(
        [DSTAT_DIR+"/dstat", "-m", "--noheaders", "--noupdate", "--nocolor", "--output", "mysql_memory_usage.csv"],
        stdout=subprocess.PIPE
      )
      time.sleep(2)

      # run query
      execution = subprocess.Popen(
        [
          "{0}/mysql/bin/mysql".format(MYSQL_DIR),
          "--defaults-file={0}".format(MY_CNF),
          "-vvv", "-u", "root", "-proot", "laolap"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE
      )

      # get query results_mysql
      output, _ = execution.communicate(input=query.format(data_size))
      output_line_list = output.splitlines()

      # stop profiler
      time.sleep(2)
      dstat.terminate()

      # get profiler results_mysql
      mem_out_list=[]
      with open("mysql_memory_usage.csv", "r") as mem_info:
        mem_out_list = mem_info.readlines()[7:-1]
      os.remove("mysql_memory_usage.csv")

      # calculate max memory usage
      memory.append(
        max(map( lambda x: float(x.split(',')[0]), mem_out_list ))
      )

      # calculate execution time
      t_str = output_line_list[-3].split('(')[1].split(')')[0].split()[::2]
      t_acc = float(t_str[0])
      for t in t_str[1:]:
        t_acc *= 60.0
        t_acc += float(t)
      exec_time.append( t_acc )


    # aggregate data from multiple runs storing the k-best, average and standard deviation
    with open("results_mysql/query_{0}_time.csv".format(num), "a") as csv_time:
      kb = round( kbest(3,exec_time), 6)
      av = round( avg(exec_time), 6)
      sd = round( sdev(av,exec_time), 6)
      csv_time.write( ",{0},{1},{2}\n".format(kb, av, sd) )
    with open("results_mysql/query_{0}_memory.csv".format(num), "a") as csv_memory:
      kb = round( kbest(3,memory), 6)
      av = round( avg(memory), 6)
      sd = round( sdev(av,memory), 6)
      csv_memory.write( ",{0},{1},{2}\n".format(kb, av, sd) )


queries = {
  "3": ("select SQL_NO_CACHE "
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
  ),

  "4": ("select SQL_NO_CACHE "
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
  ),

  "6": ("select SQL_NO_CACHE "
        "sum(l_extendedprice * l_discount) as revenue "
        "from "
        "lineitem_{0} "
        "where "
        "l_shipdate >= date '1995-03-10' "
        "and l_shipdate < date '1996-03-10' "
        "and l_discount between 0.04 and 0.06 "
        "and l_quantity < 3;"
  ),

  "11": ("select SQL_NO_CACHE "
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
  ),

  "12": ("select SQL_NO_CACHE "
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
  ),

  "14": ("select SQL_NO_CACHE "
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
  )
}

if __name__ == "__main__":
  main(sys.argv)
