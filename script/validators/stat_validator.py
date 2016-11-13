########################################
# Tested with Python 3
# Assume this sample code is running in peloton-tf/physical-stat
# 
# The sample csv file contains the physical stats for 8 queries. 
# The default delimiter between columns is comma (,) and the one between rows is new_line (\n). 
#
# The physical stat is in the format of: 
# sql, db_id, num_params, param1_type, param2_type,.., param_n_type, param1_value, param2_value, ... paramn_val, 
# reads, updates, deletes, inserts, lqqatency, cpu_time, timestamp
#
# Note that some of the query doesn't have parameters, in that case, num_params = 0, followed by reads, updates, etc.
# 
# Since the parameter of the query could also contain delimiters, peloton put \\ in front of them to escape these 
# characters. For example, you can check the insert query in sample data file with vim, which contains a newline 
# character and comma character in the parameter. 
# 
##########################################
import sys
import os
import subprocess
import time
import csv
import re
from functools import reduce

# Check python version
if sys.version_info[0] < 3:
    raise "Please use Python 3 to run this script"

####################################################
# Postgres types copied from peloton/src/include/common/types.h
####################################################
POSTGRES_VALUE_TYPE_INVALID = -1
POSTGRES_VALUE_TYPE_BOOLEAN = 16
POSTGRES_VALUE_TYPE_SMALLINT = 21
POSTGRES_VALUE_TYPE_INTEGER = 23
POSTGRES_VALUE_TYPE_VARBINARY = 17
POSTGRES_VALUE_TYPE_BIGINT = 20
POSTGRES_VALUE_TYPE_REAL = 700
POSTGRES_VALUE_TYPE_DOUBLE = 701
POSTGRES_VALUE_TYPE_TEXT = 25
POSTGRES_VALUE_TYPE_BPCHAR = 1042
POSTGRES_VALUE_TYPE_BPCHAR2 = 1014
POSTGRES_VALUE_TYPE_VARCHAR = 1015
POSTGRES_VALUE_TYPE_VARCHAR2 = 1043
POSTGRES_VALUE_TYPE_DATE = 1082
POSTGRES_VALUE_TYPE_TIMESTAMPS = 1114
POSTGRES_VALUE_TYPE_TIMESTAMPS2 = 1184
POSTGRES_VALUE_TYPE_TEXT_ARRAY = 1009     # TEXTARRAYOID in postgres code
POSTGRES_VALUE_TYPE_INT2_ARRAY = 1005     # INT2ARRAYOID in postgres code
POSTGRES_VALUE_TYPE_INT4_ARRAY = 1007     # INT4ARRAYOID in postgres code
POSTGRES_VALUE_TYPE_OID_ARRAY = 1028      # OIDARRAYOID in postgres code
POSTGRES_VALUE_TYPE_FLOADT4_ARRAY = 1021  # FLOADT4ARRAYOID in postgres code
POSTGRES_VALUE_TYPE_DECIMAL = 1700


# Parse the physical stat csv file given it's path
def parse(file_path):
    #file_path = '../dataset/sample_query_metric_table_output.csv
    file = open(file_path, newline='')
    # Parse rows
    stat_reader = csv.reader(file, delimiter='\n', escapechar='\\', quoting=csv.QUOTE_NONE)
    # Parse columns
    for stat_row in stat_reader:
        # Look behind an split only if the previous character is not \\
        row = re.split(r'(?<!\\),', stat_row[0])
        columns = list(map(lambda x: x.replace('\\,', ','), row))
        #print(columns)

        # verify result for insert stmt. Expected result:
        # ['INSERT INTO A VALUES ($1,$2)', '12345', '2', '23', '1043', '5', 'Yo\nYo,Yo', '0', '0', '0', '1', '0', '0', '1478900591']
        if "INSERT INTO" in columns[0]:
            # Check number of parameters
            assert(int(columns[2]) == 2)
            # Check the types
            assert(int(columns[3]) == POSTGRES_VALUE_TYPE_INTEGER)
            assert(int(columns[4]) == POSTGRES_VALUE_TYPE_VARCHAR2)
            # Check the values 
            try:
                expected_value = 'Yo\nYo,Yo'
                assert(columns[6] == expected_value)
            except AssertionError:
                print("Expect:\n" + expected_value + "\nFound:\n" + columns[6])
                assert(False)
    return 0

## ==============================================
## Below is an automated script to check the correctness of parse result 
## ==============================================

## CONFIGURATION
# NOTE: absolute path to peloton directory is calculated from current directory
# directory structure: peloton/scripts/validators/<this_file>
# PELOTON_DIR needs to be redefined if the directory structure is changed
CODE_SOURCE_DIR = os.path.abspath(os.path.dirname(__file__))
PELOTON_DIR = reduce(os.path.join, [CODE_SOURCE_DIR, os.path.pardir, os.path.pardir])

# Change if Peloton bin directory is modified
PELOTON_BIN = os.path.join(PELOTON_DIR, "build/bin/peloton")

# The temp file for physical stat
PELOTON_STAT_CSV = os.path.join(PELOTON_DIR, "output.csv")

# Path to JDBC test script
PELOTON_JDBC_SCRIPT_DIR = os.path.join(PELOTON_DIR, "script/testing/jdbc")

# Path to physical stat parser
PELOTON_CSV_PARSER = os.path.join(PELOTON_DIR, "script/stats/load_stat.py")

EXIT_SUCCESS = 0
EXIT_FAILURE = -1

if __name__ == '__main__':
        # Launch Peloton with Copy Option
        peloton = subprocess.Popen(['exec ' + PELOTON_BIN + ' -port 54321 -stats_mode 1'], shell=True)

        # start jdbc test
        os.chdir(PELOTON_JDBC_SCRIPT_DIR)
        jdbc = subprocess.Popen(['/bin/bash test_jdbc.sh copy ' + PELOTON_STAT_CSV], shell=True, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)
        jdbc.stdout.readlines()

        # kill peloton
        peloton.kill()

        # try parse the stat result
        parse(PELOTON_STAT_CSV)

        sys.exit(EXIT_SUCCESS)

