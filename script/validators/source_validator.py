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
import subprocess
import difflib
import mmap
import glob
import functools

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
PELOTON_DIR = os.path.abspath(
    functools.reduce(os.path.join, [CODE_SOURCE_DIR, os.path.pardir, os.path.pardir])
    )

CLANG_FORMAT = "clang-format-3.6"
CLANG_FORMAT_FILE = os.path.join(PELOTON_DIR, ".clang-format")

# Other directory paths used are relative to PELOTON_DIR
DEFAULT_DIRS = [
    os.path.join(PELOTON_DIR, "src"),
    os.path.join(PELOTON_DIR, "test")
]

# To be used in check_includes.
PATHS = set([path.replace('src/include/', '') for path in glob.glob('src/include/**/*.h')])

EXIT_SUCCESS = 0
EXIT_FAILURE = -1

# Source patterns to check for
VALIDATOR_PATTERNS = [re.compile(patt) for patt in [
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
    "\_\_attribute\_\_\(\(unused\)\)",
    r'^#include "include/'
    ]
]

# Files that should not be checked
SKIP_FILES_LIST = [
    "src/common/allocator.cpp",
    "src/network/socket_base.cpp",
    "src/network/protocol.cpp",
    "src/include/common/macros.h",
    "src/common/stack_trace.cpp",
    "src/include/parser/sql_scanner.h", # There is a free() in comments
    "src/include/index/bloom_filter.h",
    "src/include/index/compact_ints_key.h",
    "src/include/index/bwtree.h",
    "src/codegen/util/oa_hash_table.cpp",
    "src/codegen/util/cc_hash_table.cpp"
]

FORMATTING_FILE_WHITELIST = [
    # Fill me
]

## ==============================================
##           UTILITY FUNCTION DEFINITIONS
## ==============================================

def check_common_patterns(file_path):
    rel_path_from_peloton_dir = os.path.relpath(file_path, PELOTON_DIR)

    # Skip some files
    if rel_path_from_peloton_dir in SKIP_FILES_LIST:
        return True

    with open(file_path, 'r') as file:
        status = True
        line_ctr = 1
        for line in file:

            for validator_pattern in VALIDATOR_PATTERNS:
                # Check for patterns one at a time
                if validator_pattern.search(line):
                    if status:
                        LOG.info("Invalid pattern -- " + validator_pattern.pattern + " -- found in : " + file_path)
                    LOG.info("  Line #%d :: %s" % (line_ctr, line))
                    status = False
            line_ctr += 1

    return status

def check_format(file_path):
    rel_path_from_peloton_dir = os.path.relpath(file_path, PELOTON_DIR)

    if rel_path_from_peloton_dir in FORMATTING_FILE_WHITELIST:
        return True

    status = True

    # Run clang-format on the file
    try:
        clang_format_cmd = [CLANG_FORMAT, "-style=file", file_path]
        formatted_src = subprocess.check_output(clang_format_cmd).splitlines(True)
        # Load source file
        with open(file_path, "r") as file:
            src = file.readlines()

            # Do the diff
            d = difflib.Differ()
            diff = d.compare(src, formatted_src)
            line_num = 0
            for line in diff:
                code = line[:2]
                if code in ("  ", "- "):
                    line_num += 1
                if code == '- ':
                    if status:
                        LOG.info("Invalid formatting in file : " + file_path)
                    LOG.info("  Line %d: %s" % (line_num, line[2:].strip()))
                    status = False

            return status
    except OSError as e:
        LOG.error("clang-format seems not installed")
        exit()

def check_namespaces(file_path):
    # only check for src files
    if file_path.startswith(DEFAULT_DIRS[0]) == False:
        return True
    
    # get required namespaces from path
    required_namespaces = ['peloton'] + file_path.replace(DEFAULT_DIRS[0] + "/", "").split("/")
    
    # for the include files, remove the include item in the list
    if 'include' in required_namespaces:
        required_namespaces.remove('include')
        
    # cut off the file name at the end of the list
    required_namespaces = required_namespaces[:-1]

    with open(file_path, 'r') as file:
        data = mmap.mmap(file.fileno(), 0, prot=mmap.PROT_READ)
        
        # scan for all namespace openings and closings
        matches = re.findall(r'^ *namespace ([a-z_-]+) {$|^ *} +\/\/ namespace ([a-z_-]+)$', data, flags=re.MULTILINE)

        open_namespaces = list()
        namespace_errors = list()

        for match in matches:
            # assert the match is either an opening or a closing
            assert match[0] or match[1]

            # 1. namespace opening
            if match[0]:
                # add to list of open namespaces
                open_namespaces.append(match[0])

                # remove from list of required namespaces
                if required_namespaces and required_namespaces[0] == match[0]:
                    required_namespaces.pop(0)

            # 2. namespace closing
            else:
                # check if correct order
                if open_namespaces and open_namespaces[-1] != match[1]:
                    namespace_errors.append("This namespace was closed in wrong order: '" + match[1] + "' -- in " + file_path)
                # check if present at all
                if not match[1] in open_namespaces:
                    namespace_errors.append("This namespace was closed, but is missing a correct opening: '" + match[1] + "' -- in " + file_path)
                else:
                    # remove from open list
                    open_namespaces.remove(match[1])

        if required_namespaces:
            namespace_errors.append("Required namespaces are missing or in wrong order: " + str(required_namespaces) + " -- in " + file_path)

        if open_namespaces:
            namespace_errors.append("These namespaces were not closed properly: " + str(open_namespaces) + " -- in " + file_path)

        if namespace_errors:
            LOG.info("Invalid namespace style -- in " + file_path)
            for error in namespace_errors:
                LOG.info("  " + error)
            return False
        else:
            return True
    
    return status


def check_includes(file_path):
    """Checks whether local includes are done via #include<...>"""
    with open(file_path, "r") as f:
        path_pattern = re.compile(r'^#include <(include/)?(.*?)>')
        linenum = 0
        status = True
        for line in f:
            linenum += 1
            res = path_pattern.match(line)
            if res:
                path = res.groups()[1]
                if path in PATHS:
                    if status:
                        LOG.info("Invalid include in %s" % file_path)
                    status = False
                    LOG.info("Line %s: %s" % (linenum, line.strip()))
        if not status:
            LOG.info('includes for peloton header files have must not have brackets')
    return status


VALIDATORS = [
    check_common_patterns,
    check_includes,

    # Uncomment the below validator once the namespace refactoring is done
    #check_namespaces, 
    
    # Uncomment the below validator when all files are clang-format-compliant
    #check_format
]

# Validate the file passed as argument
def validate_file(file_path):
    if file_path.endswith(".h") == False and file_path.endswith(".cpp") == False:
        return True

    status = True
    for validator in VALIDATORS:
        if validator(file_path) == False:
            status = False

    return status

# Validate all the files in the dir passed as argument
def validate_dir(dir_path):

    status = True
    for subdir, dirs, files in os.walk(dir_path):
        for file in files:
            file_path = subdir + os.path.sep + file

            if validate_file(file_path) == False:
                status = False
    return status

            #END IF
        #END FOR [file]
    #END FOR [os.walk]
#END VALIDATE_DIR(DIR_PATH)

## ==============================================
##                 Main Function
## ==============================================

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Perform source code validation on Peloton source')
    parser.add_argument("-f", "--files", nargs='*', help="A list of files to validate")
    args = parser.parse_args()

    LOG.info("Running source validator ...")
    LOG.info("Peloton root : " + PELOTON_DIR)

    if args.files:
        # Validate just the provided files.
        VALIDATORS.append(check_format)  # In this mode, we perform explicit clang-format checks
        for file in args.files:
            file = os.path.abspath(file.lower())

            # Fail if the file isn't really a file
            if os.path.isfile(file) == False:
                LOG.info("ERROR: " + file + " isn't a file")
                sys.exit(EXIT_FAILURE)

            # Skip files not in 'src' or 'test'
            if file.startswith(DEFAULT_DIRS[0]) == False and file.startswith(DEFAULT_DIRS[1]) == False:
                LOG.info("Skipping non-Peloton source : " + file)
                continue

            # Looks good, let's validate
            LOG.info("Scanning file : " + file)
            status = validate_file(file)
            if status == False:
                LOG.info("Validation NOT successful")
                sys.exit(EXIT_FAILURE)
        #END FOR

    else:
        ## Validate all files in source and test directories
        for dir in DEFAULT_DIRS:
            LOG.info("Scanning directory : " + dir)

            status = validate_dir(dir)
            if status == False:
                LOG.info("Validation NOT successful")
                sys.exit(EXIT_FAILURE)
        #END FOR

    LOG.info("Validation successful")
    sys.exit(EXIT_SUCCESS)
