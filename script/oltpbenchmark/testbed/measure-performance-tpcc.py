# profile tpcc benchmark.

import os
import sys
import time
import subprocess
import re
import argparse
from subprocess import call
from pprint import pprint

cwd = os.getcwd()
OLTP_HOME = "%s/oltpbench" % (cwd)

db_host = None
db_port = None

thread_num = 1
new_order_ratio = 100
payment_ratio = 0
order_status_ratio = 0
delivery_ratio = 0
stock_level_ratio = 0
SCALE_FACTOR = 1
TEST_TIME = 20
# TEST_TIME = 60

# Requires that peloton-testbed is in the home directory
config_filename = "tpcc_config.xml"
start_benchmark_script = "%s/oltpbenchmark -b tpcc -c " % (OLTP_HOME) + \
                         cwd + "/" + config_filename + " --histograms  --create=true --load=true --execute=true -s 5 -o "

parameters={}

def prepare_parameters():
    os.chdir(cwd)
    parameters["$THREAD_NUMBER"] = str(thread_num)
    parameters["$NEW_ORDER_RATIO"] = str(new_order_ratio)
    parameters["$PAYMENT_RATIO"] = str(payment_ratio)
    parameters["$ORDER_STATUS_RATIO"] = str(order_status_ratio)
    parameters["$DELIVERY_RATIO"] = str(delivery_ratio)
    parameters["$STOCK_LEVEL_RATIO"] = str(stock_level_ratio)
    parameters["$SCALE_FACTOR"] = str(SCALE_FACTOR)
    parameters["$IP"] = db_host
    parameters["$PORT"] = db_port
    parameters["$TIME"] = str(TEST_TIME)

    template = ""
    with open("tpcc_template.xml") as in_file:
        template = in_file.read()
    for param,value in parameters.items():
        template = template.replace(param, value)
    with open(config_filename, "w") as out_file:
        out_file.write(template)

def get_result_path():
    global cwd
    #base_dir = "%s/results/" % cwd
    #if not os.path.exists(base_dir):
        #os.mkdir(base_dir)
    #return base_dir +
    return "outputfile_%d_%d_%d_%d_%d_%d" % (new_order_ratio, payment_ratio, order_status_ratio, delivery_ratio, stock_level_ratio, SCALE_FACTOR)

def start_bench():
    global cwd
    # go to oltpbench directory
    os.chdir(os.path.expanduser(OLTP_HOME))
    # call("git pull origin master", shell=True)
    
    resultsDir = "results"
    if not os.path.exists(resultsDir):
        os.mkdir(resultsDir)
    
    call("ant", shell=True)
    cmd = start_benchmark_script + get_result_path() + " 2>&1 | tee results/%s.log" % (get_result_path())
    print cmd
    call(cmd, shell=True)

def collect_data(result_dir_name):
    os.chdir(cwd)
    os.system("rm -rf " + result_dir_name)
    os.system("mkdir " + result_dir_name)
    os.system("mv callgrind.out.* " + result_dir_name)
    call("cat %s/results/%s.log" % (OLTP_HOME, get_result_path()), shell=True)
    os.system("mv %s/results/%s.* %s/" % (OLTP_HOME, get_result_path(), result_dir_name))

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(description='Timeseries')
    aparser.add_argument('db-host', help='DB Hostname')
    aparser.add_argument('db-port', type=int, help='DB Port')
    aparser.add_argument('--client-threads', type=int, metavar='N', \
                         help='Number of client threads to use')
    aparser.add_argument('--client-time', type=int, metavar='T', \
                         help='How long to execute each benchmark trial')
    args = vars(aparser.parse_args())
    
    ## ----------------------------------------------
    ## ARGUMENT PROCESSING 
    ## ----------------------------------------------
    
    db_host = str(args['db-host'])
    db_port = str(args['db-port'])
    
    # If we are given a specific client thread count, then we will just test that
    thread_counts = None
    if "client_threads" in args and args["client_threads"]:
        thread_counts = [ int(args["client_threads"]) ]
    # Otherwise we will sweep across these numbers
    else:
        thread_counts = [1, 2, 4, 6, 8, 10, 16, 20, 24, 28, 32, 40, 48, 56, 64]
    
    if "client_time" in args and args["client_time"]:
        TEST_TIME = int(args["client_time"])
    
    
    # 45:43:4:4:4
    # thread_num = 20
    new_order_ratio = 45
    payment_ratio = 43
    order_status_ratio = 4
    delivery_ratio = 4
    stock_level_ratio = 4

    ## ----------------------------------------------
    ## EXECUTE
    ## ----------------------------------------------
    for thread_num in thread_counts:
        SCALE_FACTOR = thread_num
        prepare_parameters()
        start_bench()
        result_dir_name = "tpcc_collected_data_%d_%d_%d_%d_%d_%d_%d" % (new_order_ratio, payment_ratio, order_status_ratio, delivery_ratio, stock_level_ratio, SCALE_FACTOR, thread_num)
        collect_data(result_dir_name)
## MAIN
