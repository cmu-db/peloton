# profile ycsb benchmark.
# author: Yingjun Wu <yingjun@comp.nus.edu.sg>
# date: March 5th, 2016

import os
import sys
import time
import subprocess
import re
from subprocess import call

DB_HOST = "dev1.db.pdl.cmu.local"
DB_PORT = '57721'
DB_USER = 'postgres'
DB_PASSWD = 'postgres'

cwd = os.getcwd()
OLTP_HOME = "%s/oltpbench" % (cwd)

parameters = {
"$IP":  "localhost",
"$PORT": "57721",
"$SCALE_FACTOR": "100",
"$TIME":  "60",
"$THREAD_NUMBER": "1",
"$READ_RATIO": "0",
"$INSERT_RATIO": "0",
"$SCAN_RATIO": "0",
"$UPDATE_RATIO": "0",
"$DELETE_RATIO": "0",
"$RMW_RATIO": "0"
}

contention = 0.00
read_ratio = 0
insert_ratio = 0
update_ratio = 100
rmw_ratio = 0
thread_num = 24
scale_factor = 10

config_filename = "timesten_ycsb.xml"

def prepare_parameters():
    os.chdir(cwd)
    parameters["$SCALE_FACTOR"] = str(scale_factor)
    parameters["$THREAD_NUMBER"] = str(thread_num)
    parameters["$READ_RATIO"] = str(read_ratio)
    parameters["$INSERT_RATIO"] = str(insert_ratio)
    parameters["$UPDATE_RATIO"] = str(update_ratio)
    parameters["$RMW_RATIO"] = str(rmw_ratio)
    parameters["$IP"] = DB_HOST
    parameters["$PORT"] = DB_PORT
    ycsb_template = ""
    with open("ycsb_template.xml") as in_file:
        ycsb_template = in_file.read()
    for param in parameters:
        ycsb_template = ycsb_template.replace(param, parameters[param])
    with open(config_filename, "w") as out_file:
        out_file.write(ycsb_template)

    os.system("sed -i 's/ZIPFIAN_CONSTANT=.*\?;/ZIPFIAN_CONSTANT=%.5f;/' %s" % (contention, OLTP_HOME + "/src/com/oltpbenchmark/distributions/ZipfianGenerator.java"))

def get_result_path():
  global cwd
  return "%s/outputfile_%.2f_%d_%d_%d_%d" % (cwd, contention, read_ratio, insert_ratio, update_ratio, rmw_ratio)

def start_bench():
    # go to oltpbench directory
    os.chdir(os.path.expanduser(OLTP_HOME))
    call("git pull origin master", shell=True)
    call("ant", shell=True)
    start_ycsb_bench_script = "%s/oltpbenchmark -b ycsb -c " % (OLTP_HOME) + cwd + "/" + config_filename + " --histograms --create=true --load=true --execute=true -s 5 -o "
    cmd = start_ycsb_bench_script + get_result_path() + " 2>&1 | tee %s.log" % (get_result_path())
    call(cmd, shell=True)
    call("cat %s.log" % (get_result_path()), shell=True)

def collect_data(result_dir_name):
    os.chdir(cwd)
    os.system("rm -rf " + result_dir_name)
    os.system("mkdir " + result_dir_name)
    os.system("mv callgrind.out.* " + result_dir_name)
    os.system("mv %s.* %s/" % (get_result_path(), result_dir_name))

# R/W: 0/100, 10/90, 30/70, 50/50, 70/30, 100/0 
# Cont: 0, 0.1, 0.3, 0.5, 0.9, 0.99
# Thr: 1-12
if __name__ == "__main__":
  contention = 0.3
  read_ratio = 100
  thread_num = 1
  insert_ratio = 0
  update_ratio = 0
  rmw_ratio = 0
  SCALE_FACTOR = 10

  for r in [10, 100]:
    for th in [1, 2, 4, 8, 10]:
      thread_num = th
      read_ratio = r
      update_ratio = 100 - read_ratio
      prepare_parameters()
      start_bench()
      collect_data("ycsb_collected_data" + "_"+ "%.2f" % (contention) + "_" + str(read_ratio) + "_" + str(insert_ratio) + "_" + str(update_ratio) + "_" + str(rmw_ratio) + "_" + str(thread_num))

