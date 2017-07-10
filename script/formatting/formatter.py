#!/usr/bin/env python
# encoding: utf-8

## ==============================================
## GOAL : Format code, Add/Strip headers
## ==============================================

import argparse
import logging
import os
import re
import sys
import datetime

## ==============================================
## CONFIGURATION
## ==============================================

# NOTE: absolute path to peloton directory is calculated from current directory
# directory structure: peloton/scripts/formatting/<this_file>
# PELOTON_DIR needs to be redefined if the directory structure is changed
CODE_SOURCE_DIR = os.path.abspath(os.path.dirname(__file__))
PELOTON_DIR = reduce(os.path.join, [CODE_SOURCE_DIR, os.path.pardir, os.path.pardir])

#other directory paths used are relative to peloton_dir
PELOTON_SRC_DIR = os.path.join(PELOTON_DIR, "src")
PELOTON_TESTS_DIR = os.path.join(PELOTON_DIR, "test")

# DEFAULT DIRS
DEFAULT_DIRS = []
DEFAULT_DIRS.append(PELOTON_SRC_DIR)
DEFAULT_DIRS.append(PELOTON_TESTS_DIR)

CLANG_FORMAT = "clang-format-3.6"

## ==============================================
##             HEADER CONFIGURATION
## ==============================================

#header framework, dynamic information will be added inside function
header_comment_line_1 = "//===----------------------------------------------------------------------===//\n"
header_comment_line_1 += "//\n"
header_comment_line_1 += "//                         Peloton\n"
header_comment_line_2  = "//\n"
header_comment_line_3  = "// "
header_comment_line_4  = "//\n"
header_comment_line_5  = "// Identification: "
header_comment_line_6  = "//\n"
header_comment_line_7  = "// Copyright (c) 2015-%d, Carnegie Mellon University Database Group\n" % datetime.datetime.now().year
header_comment_line_8  = "//\n"
header_comment_line_9  = "//===----------------------------------------------------------------------===//\n\n\n"

header_comment_1 = header_comment_line_1 + header_comment_line_2
header_comment_3 = header_comment_line_4
header_comment_5 = header_comment_line_6 + header_comment_line_7 + header_comment_line_8 \
                        + header_comment_line_9

#regular expresseion used to track header
header_regex = re.compile("((\/\/===-*===\/\/\n(\/\/.*\n)*\/\/===-*===\/\/[\n]*)\n\n)*")

## ==============================================
##             LOGGING CONFIGURATION
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
##           UTILITY FUNCTION DEFINITIONS
## ==============================================

#format the file passed as argument
def format_file(file_path, add_header, strip_header, clang_format_code):

    file_name = os.path.basename(file_path)
    abs_path = os.path.abspath(file_path)
    rel_path_from_peloton_dir = os.path.relpath(abs_path,PELOTON_DIR)

    with open(file_path, "r+") as fd:
        file_data = fd.read()

        if add_header:
            header_comment_2 = header_comment_line_3 + file_name + "\n"
            header_comment_4 = header_comment_line_5 + rel_path_from_peloton_dir + "\n"
            header_comment = header_comment_1 + header_comment_2 + header_comment_3 \
                        + header_comment_4 + header_comment_5
            #print header_comment

            new_file_data = header_comment + file_data

            fd.seek(0,0)
            fd.truncate()
            fd.write(new_file_data)

        elif strip_header:

            LOG.info("Strip %s", file_name)
            
            header_match = header_regex.match(file_data)
            if header_match is None:
              return

            LOG.debug("Header match ")

            header_comment = header_match.group()
            
            LOG.debug("Header comment : %s", header_comment)
        
            new_file_data = file_data.replace(header_comment,"")

            fd.seek(0,0)
            fd.truncate()
            fd.write(new_file_data)

        elif clang_format_code:
            formatting_command = CLANG_FORMAT + " -style=file " + " -i " + file_path
            LOG.info(formatting_command)
            os.system(formatting_command)

    #END WITH

    fd.close()
#END FORMAT__FILE(FILE_NAME)


#format all the files in the dir passed as argument
def format_dir(dir_path, add_header, strip_header, clang_format_code):
    for subdir, dirs, files in os.walk(dir_path):
        for file in files:
            #print os.path.join(subdir, file)
            file_path = subdir + os.path.sep + file

            if file_path.endswith(".h") or file_path.endswith(".cpp"):
                format_file(file_path, add_header, strip_header, clang_format_code)
            #END IF
        #END FOR [file]
    #END FOR [os.walk]
#END ADD_HEADERS_DIR(DIR_PATH)

## ==============================================
##                 Main Function
## ==============================================

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Add/delete headers and/or format source code')

    parser.add_argument("-a", "--add-header", help='Action: Add suitable header(s)', action='store_true')
    parser.add_argument("-s", "--strip-header", help='Action: Strip existing header(s)', action='store_true')
    parser.add_argument("-c", "--clang-format-code", help='Action: Apply clang-format to source code', action='store_true')
    parser.add_argument('paths', metavar='PATH', type=str, nargs='+',
                        help='Files or directories to (recursively) apply the actions to')
    
    args = parser.parse_args()

    # SOME SANITY CHECKS
    if args.add_header and args.strip_header:
        LOG.error("adding and stripping headers cannot be done together -- exiting")
        sys.exit("adding and stripping headers cannot be done together")

    # If there are no paths given, then we will scan the defaults
    # PAVLO 2017-07-09: ^^^ The above logic seems like a bad idea
    targets = DEFAULT_DIRS if not args.paths else args.paths
    for x in targets:
        if os.path.isfile(x):
            LOG.info("Scanning file: " + x)
            format_file(x, args.add_header, args.strip_header, args.clang_format_code)
        elif os.path.isdir(x):
            LOG.info("Scanning directory " + x)
            format_dir(x, args.add_header, args.strip_header, args.clang_format_code)
    ## FOR
## IF
        
