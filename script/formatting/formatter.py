#!/usr/bin/env python
# encoding: utf-8

## ==============================================
## GOAL : Format code, Update headers
## ==============================================

import argparse
import logging
import os
import re
import sys
import datetime
import subprocess

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
CLANG_FORMAT_FILE = os.path.join(PELOTON_DIR, ".clang-format")

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
header_comment_line_9  = "//===----------------------------------------------------------------------===//\n\n"

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
def format_file(file_path, update_header, clang_format_code):

    file_name = os.path.basename(file_path)
    abs_path = os.path.abspath(file_path)
    rel_path_from_peloton_dir = os.path.relpath(abs_path,PELOTON_DIR)

    with open(file_path, "r+") as fd:
        file_data = fd.read()

        if update_header:
            # strip old header if it exists
            header_match = header_regex.match(file_data)
            if not header_match is None:
              LOG.info("Strip header from %s", file_name)
              header_comment = header_match.group()
              LOG.debug("Header comment : %s", header_comment)
              file_data = file_data.replace(header_comment,"")
            
            # add new header
            LOG.info("Add header to %s", file_name)
            header_comment_2 = header_comment_line_3 + file_name + "\n"
            header_comment_4 = header_comment_line_5 + rel_path_from_peloton_dir + "\n"
            header_comment = header_comment_1 + header_comment_2 + header_comment_3 \
                        + header_comment_4 + header_comment_5
            #print header_comment

            file_data = header_comment + file_data

            fd.seek(0,0)
            fd.truncate()
            fd.write(file_data)

        elif clang_format_code:
            try:
                formatting_command = CLANG_FORMAT + " -style=file -i " + file_path
                LOG.info(formatting_command)
                subprocess.call([CLANG_FORMAT, "-style=file", "-i", file_path])
            except OSError as e:
                LOG.error("clang-format seems not installed")
                exit("clang-format seems not installed")

    #END WITH

    fd.close()
#END FORMAT__FILE(FILE_NAME)


#format all the files in the dir passed as argument
def format_dir(dir_path, update_header, clang_format_code):
    for subdir, dirs, files in os.walk(dir_path):
        for file in files:
            #print os.path.join(subdir, file)
            file_path = subdir + os.path.sep + file

            if file_path.endswith(".h") or file_path.endswith(".cpp"):
                format_file(file_path, update_header, clang_format_code)
            #END IF
        #END FOR [file]
    #END FOR [os.walk]
#END ADD_HEADERS_DIR(DIR_PATH)

## ==============================================
##                 Main Function
## ==============================================

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Update headers and/or format source code')

    parser.add_argument("-u", "--update-header", help='Action: Update existing headers or add new ones', action='store_true')
    parser.add_argument("-c", "--clang-format-code", help='Action: Apply clang-format to source code', action='store_true')
    parser.add_argument("-f", "--staged-files", help='Action: Apply the selected action(s) to all staged files (git)', action='store_true')
    parser.add_argument('paths', metavar='PATH', type=str, nargs='*',
                        help='Files or directories to (recursively) apply the actions to')
    
    args = parser.parse_args()

    if args.staged_files:
        targets = [os.path.abspath(os.path.join(PELOTON_DIR, f)) for f in subprocess.check_output(["git", "diff", "--name-only", "HEAD", "--cached", "--diff-filter=d"]).split()]
        if not targets:
            LOG.error("no staged files or not calling from a repository -- exiting")
            sys.exit("no staged files or not calling from a repository")
    elif not args.paths:
        LOG.error("no files or directories given -- exiting")
        sys.exit("no files or directories given")
    else:
        targets = args.paths
    
    for x in targets:
        if os.path.isfile(x):
            LOG.info("Scanning file: " + x)
            format_file(x, args.update_header, args.clang_format_code)
        elif os.path.isdir(x):
            LOG.info("Scanning directory " + x)
            format_dir(x, args.update_header, args.clang_format_code)
    ## FOR
## IF
        
