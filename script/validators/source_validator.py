#!/usr/bin/env python
# encoding: utf-8

## ==============================================
## GOAL : Check for std::cout and printf and avoid them
## ==============================================

import argparse
import logging
import os
import re
import sys
import pprint

## ==============================================
## LOGGING CONFIGURATION
## ==============================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S'
)
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)


## ==============================================
## CONFIGURATION
## ==============================================

# NOTE: absolute path to peloton directory is calculated from current directory
# directory structure: peloton/scripts/formatting/<this_file>
# PELOTON_DIR needs to be redefined if the directory structure is changed
CODE_SOURCE_DIR = os.path.abspath(os.path.dirname(__file__))
PELOTON_DIR = reduce(os.path.join, [CODE_SOURCE_DIR, os.path.pardir, os.path.pardir])

#other directory paths used are relative to PELOTON_DIR
DEFAULT_DIRS = [
    os.path.join(PELOTON_DIR, "src"),
    os.path.join(PELOTON_DIR, "test")
]

EXIT_SUCCESS = 0
EXIT_FAILURE = -1

VALIDATOR_PATTERNS = [
    "std\:\:cout",
    " printf\(",
    "cout",
    " malloc\(",
    " free\(",
    " memset\(",
    " memcpy\(",
    " \:\:memset\(",
    " \:\:memcpy\(",
    " std\:\:memset\(",
    " std\:\:memcpy\(",
    "\_\_attribute\_\_\(\(unused\)\)"
]

SKIP_FILES_LIST = [
    "src/common/allocator.cpp",
    "src/wire/socket_base.cpp",
    "src/wire/protocol.cpp",
    "src/include/common/macros.h",
    "src/common/stack_trace.cpp",
    "src/include/parser/sql_scanner.h", # There is a free() in comments
    "src/parser/parser_utils.cpp",
    "src/include/parser/sql_statement.h",
    "src/include/parser/create_statement.h",
    "src/include/parser/delete_statement.h",
    "src/include/parser/drop_statement.h",
    "src/include/parser/execute_statement.h",
    "src/include/parser/import_statement.h",
    "src/include/parser/insert_statement.h",
    "src/include/parser/prepare_statement.h",
    "src/include/parser/update_statement.h",
    "src/parser/table_ref.cpp",
    "src/include/index/bloom_filter.h",
    "src/include/index/compact_ints_key.h",
    "src/include/index/bwtree.h",
    "src/codegen/util/oa_hash_table.cpp",
    "src/codegen/util/cc_hash_table.cpp"
]

## ==============================================
##           UTILITY FUNCTION DEFINITIONS
## ==============================================

#validate the file passed as argument
def validate_file(file_path):

    file_name = os.path.basename(file_path)
    abs_path = os.path.abspath(file_path)
    rel_path_from_peloton_dir = os.path.relpath(abs_path, PELOTON_DIR)

    # Skip some files
    if rel_path_from_peloton_dir in SKIP_FILES_LIST:
        return True

    file = open(abs_path, "r")

    for line in file:
        for validator_pattern in VALIDATOR_PATTERNS:
            # Check for patterns one at a time
            if re.search(validator_pattern, line):
                LOG.info("Invalid pattern -- " + validator_pattern + " -- found in : " + file_path)
                LOG.info("Line :: " + line)
                return False

    file.close()
    return True

#validate all the files in the dir passed as argument
def validate_dir(dir_path):

    for subdir, dirs, files in os.walk(dir_path):
        for file in files:
            file_path = subdir + os.path.sep + file

            if file_path.endswith(".h") or file_path.endswith(".cpp"):
                status = validate_file(file_path)
                if status == False:
                    return False

            #END IF
        #END FOR [file]
    #END FOR [os.walk]
#END ADD_HEADERS_DIR(DIR_PATH)

## ==============================================
##                 Main Function
## ==============================================

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Check for some strings')
    args = parser.parse_args()

    ## VALIDATE!!!
    for dir in DEFAULT_DIRS:
        LOG.info("Scanning : " + dir)

        status = validate_dir(dir)
        if status == False:
            LOG.info("Validation not successful")
            sys.exit(EXIT_FAILURE)

    LOG.info("Validation successful")
    sys.exit(EXIT_SUCCESS)
## MAIN

