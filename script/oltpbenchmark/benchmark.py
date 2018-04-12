# profile benchmark.

import os
import sys
import time as t
import subprocess
import re
import argparse
from xml.etree import ElementTree
from subprocess import call, Popen
from pprint import pprint

os.chdir(os.path.expanduser("~"))
cwd = os.getcwd()
OLTP_HOME = "%s/oltpbench" % (cwd)
PELOTON_HOME = "%s/peloton" % (cwd)
OLTPBENCH_URL = "https://github.com/oltpbenchmark/oltpbench"
CONFIG_FILES_HOME = "%s/config/" % (OLTP_HOME)

db_host = None
db_port = None

peloton_proc=None

def run_peloton():
    global peloton_proc
    peloton_proc = Popen([PELOTON_HOME+"/build/bin/peloton"])
    t.sleep(5)

def gather_oltpbench():
    call("git clone %s" % (OLTPBENCH_URL), shell=True)

def cleanup():
    peloton_proc.kill()
    call("rm -rf %s" % (OLTP_HOME), shell=True)

def config_xml_file(host, port, benchmark, transaction, scalefactor, terminals, time, weights, upload_url, upload_code):
    xml = ElementTree.parse("%ssample_" % (CONFIG_FILES_HOME)+benchmark+"_config.xml")
    root = xml.getroot()
    root.find("dbtype").text = "peloton"
    root.find("driver").text = "org.postgresql.Driver"
    root.find("DBUrl").text = "jdbc:postgresql://{0}:{1}/{2}".format(host, port, benchmark) #host, port and benchmark name
    root.find("username").text = "postgres"
    root.find("password").text = "postgres"
    root.find("isolation").text = str(transaction) 
    root.find("scalefactor").text = str(scalefactor)
    root.find("terminals").text = str(terminals)
    root.find("uploadCode").text = str(upload_code)
    root.find("uploadUrl").text = str(upload_url)
    for work in root.find('works').findall('work'):
        work.find('time').text = str(time) 
        work.find('rate').text ="unlimited"
        work.find('weights').text = str(weights) 

    xml.write(CONFIG_FILES_HOME+benchmark+'_config.xml')



def get_result_path(weights, scalefactor):
    return "outputfile_" +weights.replace(",","_")+"_"+str(scalefactor)

def start_bench(benchmark, weights, scalefactor):
    global cwd
    os.chdir(os.path.expanduser(OLTP_HOME))
    resultsDir = "results"
    if not os.path.exists(resultsDir):
        os.mkdir(resultsDir)
    
    call("ant", shell=True)
    
    start_benchmark_script = "%s/oltpbenchmark " % (OLTP_HOME) +" -b "+ str(benchmark) +" -c " + \
                         CONFIG_FILES_HOME+benchmark+"_config.xml"+" --histograms  --create=true --load=true --execute=true -s 5 -o "
    print start_benchmark_script
    cmd = start_benchmark_script + get_result_path(weights, scalefactor) + " 2>&1 | tee results/%s.log" % (get_result_path(weights, scalefactor))
    print cmd
    call(cmd, shell=True)

def collect_data(result_dir_name, weights, scalefactor):
    os.chdir(cwd)
    os.system("rm -rf " + result_dir_name)
    os.system("mkdir " + result_dir_name)
    os.system("mv callgrind.out.* " + result_dir_name)
    call("cat %s/results/%s.log" % (OLTP_HOME, get_result_path(weights, scalefactor)), shell=True)
    os.system("mv %s/results/%s.* %s/" % (OLTP_HOME, get_result_path(weights, scalefactor), result_dir_name))

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(description='Timeseries')
    aparser.add_argument('db-host', help='DB Hostname')
    aparser.add_argument('db-port', type=int, help='DB Port')
    aparser.add_argument('benchmark', help='Benchmark Type')
    aparser.add_argument('weights', help="Benchmark weights")
    aparser.add_argument('--upload-code', type=str, \
                         help='Upload code.')
    aparser.add_argument('--upload-url', type=str, \
                         help='Upload url. (default: https://oltpbench.cs.cmu.edu/new_result/)')
    aparser.add_argument('--scale-factor', type=int, metavar='S', \
                         help='The scale factor. (default: 1)')
    aparser.add_argument('--transaction-isolation', metavar='I', \
                         help='The transaction isolation level (default: TRANSACTION_SERIALIZABLE')
    aparser.add_argument('--client-time', type=int, metavar='C', \
                         help='How long to execute each benchmark trial (default: 20)')
    aparser.add_argument('--terminals', type=int, metavar='T', \
                         help='Number of terminals in each benchmark trial (default: 1)')
    args = vars(aparser.parse_args())
    
    ## ----------------------------------------------
    ## ARGUMENT PROCESSING 
    ## ----------------------------------------------
    
    db_host = str(args['db-host'])
    db_port = str(args['db-port'])
    benchmark = str(args['benchmark'])
    weights = str(args['weights'])
    time = 20
    scalefactor=1
    transaction_isolation = "TRANSACTION_SERIALIZABLE"
    terminals = 1
    upload_code = ''
    upload_url = r'https://oltpbench.cs.cmu.edu/new_result/'

    if("upload_code" in args and args["upload_code"]):
        upload_code = str(args["upload_code"])

    if("upload_url" in args and args["upload_url"]):
        upload_url = str(args["upload_url"])

    if("client_time" in args and args["client_time"]):
        time = int(args["client_time"])
    
    if("scale_factor" in args and args["scale_factor"]):
        scalefactor = int(args["scale_factor"])
    
    if("terminals" in args and args["terminals"]):
        terminals = int(args["terminals"])
    
    if("transaction_isolation" in args and args["transaction_isolation"]):
        transaction_isolation = str(args["transaction_isolation"])
    
    run_peloton()
    gather_oltpbench()
    ## ----------------------------------------------
    ## EXECUTE
    ## ----------------------------------------------
    config_xml_file(db_host, db_port, benchmark, transaction_isolation, scalefactor, terminals, time, weights, upload_url, upload_code)
    start_bench(benchmark, weights,scalefactor)
    result_dir_name = "tpcc_collected_data_"+weights.replace(",","_")+"_"+str(scalefactor)
    collect_data(result_dir_name,weights,scalefactor)
    cleanup()
## MAIN

