#!/usr/bin/env python

import os
import re
import sys
import socket
import time
import psycopg2
from optparse import OptionParser
from config import *
from subprocess import Popen

def extract_sql(input_path, output_path):
    infile = open(input_path)
    outfile = open(output_path, "w")

    new_statement = True
    wait_for_blank = False
    query = ""

    for line in infile:
        line = line.strip() + " "
        if line == " ":
            if not wait_for_blank:
                outfile.write(query + ";\n")
            new_statement = True
            wait_for_blank = False
            query = ""
            continue
        
        if wait_for_blank:
            continue
        
        l = line.strip().split()
        if l[0] in ['#', 'statement', 'query', 'halt', 'hash-threshold']:
            continue
        if l[0] in ['onlyif', 'skipif']:
            wait_for_blank = True
            continue
        if l[0] == '----':
            outfile.write(query + ";\n")
            wait_for_blank = True
            continue
        
        # Multi-line query
        query += line
        for kw in kw_filter:
            if re.search(kw, line.upper()):
                wait_for_blank = True
                break
    ## FOR

    infile.close()
    outfile.close()
# DEF

def get_pg_jdbc():
    return "jdbc:postgresql://127.0.0.1:5433/%s" % pg_database
# DEF

def get_peloton_jdbc():
    return "jdbc:postgresql://127.0.0.1:%s/" % peloton_port
# DEF

def gen_config(path):
    conf = open(path, "w")

    conf.write(get_peloton_jdbc() + "\n")
    conf.write(peloton_username + "\n")
    conf.write(peloton_password + "\n")

    conf.write(get_pg_jdbc() + "\n")
    conf.write(pg_username + "\n")
    conf.write(pg_password + "\n")
# DEF

def wait_for_peloton_ready():
    peloton_ready = False
    while not peloton_ready:
        try:
            sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sk.settimeout(1)
            sk.connect(('127.0.0.1', peloton_port))
            peloton_ready = True
            sk.close()
        except Exception:
            pass
        if not peloton_ready:
            time.sleep(1)
# DEF

def drop_all_tables(conn):
    cur = conn.cursor()
    cur.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
    rows = cur.fetchall()
    for row in rows:
        print "dropping table: ", row[1]
        cur.execute("drop table " + row[1] + " cascade")
        cur.close()
    conn.commit()
## DEF
      

if __name__ == "__main__":
    global peloton_path, output_dir, peloton_log_file


    work_path = os.path.abspath(".")
    output_dir = os.path.abspath(output_dir)
    sql_file = os.path.abspath(os.path.join(output_dir, "test.sql"))
    config = os.path.abspath(os.path.join(output_dir, "config"))
    peloton_log_file = os.path.abspath(os.path.join(output_dir, peloton_log_file))


    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    gen_config(config)

    peloton_log = open(peloton_log_file, "w")
    peloton = None

    parser = OptionParser()
    parser.add_option("--config", dest="config", help="config file path for the testing framework", default=config)
    parser.add_option("--out", dest="out", help="Output path for JUnit XML files", default=output_dir)
    (options, args) = parser.parse_args()

    try:
        # Open a connection to postgres so that we can drop the tables
        conn = psycopg2.connect(
                dbname = pg_database,
                user = pg_username,
                password = pg_password,
                port = 5433)

        # Extract the SQL from the trace files
        for path in traces:
            print "="*80
            print "test: ", path
            print "="*80
            sys.stdout.flush()

            # Always drop the tables in the database first
            drop_all_tables(conn)

            # Convert the trace file into a format that we can handle
            extract_sql(path, sql_file)

            # Start peloton
            peloton = Popen([peloton_path, "-port", str(peloton_port)],
                            stdout=peloton_log, stderr=peloton_log)
            wait_for_peloton_ready()

            # Run the test
            os.chdir(peloton_test_path)
            test = Popen(["bash", "bin/peloton-test", "-config", options.config, "-trace", sql_file, "-out", options.out, "-batchsize", "100"])
            test.wait()
            os.chdir(work_path)

            peloton.kill()
            peloton = None
            # FOR
    except Exception as e:
        raise
    finally:
        if peloton is not None: peloton.kill()
      
