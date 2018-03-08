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

  QUERIES_DIR=os.environ["QUERIES_DIR"]
  DSTAT_DIR=os.environ["DSTAT_DIR"]
  QUERIES_STRING=os.environ["QUERIES_STRING"]

  queries_keys = QUERIES_STRING.split(",")

  for num in queries_keys:
    with open("results_la/query_{0}_time.csv".format(num), "a") as csv_time:
      csv_time.write(data_size)
    with open("results_la/query_{0}_memory.csv".format(num), "a") as csv_memory:
      csv_memory.write(data_size)

  for num in queries_keys:
    query = queries[num]

    exec_time = []
    memory = []

    for i in range(10):

      dstat = subprocess.Popen(
        [DSTAT_DIR+"/dstat", "-m", "--noheaders", "--noupdate", "--nocolor", "--output", "la_memory_usage.csv"],
        stdout=subprocess.PIPE
      )
      time.sleep(2)

      # run query
      execution = subprocess.Popen(
        [
          "{0}/{1}".format(QUERIES_DIR,query),
          data_size
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE
      )

      # get query results_la
      output, _ = execution.communicate(input=query.format(data_size))
      output_line_list = output.splitlines()

      # stop profiler
      time.sleep(2)
      dstat.terminate()

      # get profiler results_la
      mem_out_list=[]
      with open("la_memory_usage.csv", "r") as mem_info:
        mem_out_list = mem_info.readlines()[7:-1]
      os.remove("la_memory_usage.csv")

      # calculate max memory usage
      memory.append(
        max(map( lambda x: float(x.split(',')[0]), mem_out_list ))
      )


      # calculate execution time
      t_acc = float(output_line_list[-2].split('\t')[1])
      exec_time.append( t_acc )

      print t_acc


    # aggregate data from multiple runs storing the k-best, average and standard deviation
    with open("results_la/query_{0}_time.csv".format(num), "a") as csv_time:
      kb = round( kbest(3,exec_time), 6)
      av = round( avg(exec_time), 6)
      sd = round( sdev(av,exec_time), 6)
      csv_time.write( ",{0},{1},{2}\n".format(kb, av, sd) )
    with open("results_la/query_{0}_memory.csv".format(num), "a") as csv_memory:
      kb = round( kbest(3,memory), 6)
      av = round( avg(memory), 6)
      sd = round( sdev(av,memory), 6)
      csv_memory.write( ",{0},{1},{2}\n".format(kb, av, sd) )

queries = {
  "3": "query_3_seq",
  "4": "query_4",
  "6": "query_6_seq",
  "11": "query_11",
  "12": "query_12",
  "14": "query_14"
}

if __name__ == "__main__":
  main(sys.argv)
