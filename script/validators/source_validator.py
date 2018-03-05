#!/usr/bin/env python3
"""Validates source files and reports compliance anomalies."""

import argparse
import os
import re
import sys
import mmap
import glob

# Following is done so that we can import from ../helpers.py
sys.path.append(
    os.path.abspath(os.path.dirname(__file__)).replace('/validators', '')
    )
from helpers import clang_check, PELOTON_DIR, LOG

## ==============================================
## CONFIGURATION
## ==============================================

# NOTE: absolute path to peloton directory is calculated from current directory
# directory structure: peloton/scripts/formatting/<this_file>
# PELOTON_DIR needs to be redefined if the directory structure is changed

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
    r"std\:\:cout",
    r" printf\(",
    r"cout",
    r" malloc\(",
    r" free\(",
    r" memset\(",
    r" memcpy\(",
    r" \:\:memset\(",
    r" \:\:memcpy\(",
    r" std\:\:memset\(",
    r" std\:\:memcpy\(",
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

## ==============================================
##           UTILITY FUNCTION DEFINITIONS
## ==============================================


def check_common_patterns(file_path):
    """Checks for unwanted patterns in source files."""
    rel_path_from_peloton_dir = os.path.relpath(file_path, PELOTON_DIR)

    # Skip some files
    if rel_path_from_peloton_dir in SKIP_FILES_LIST:
        return True

    with open(file_path, 'r') as opened_file:
        file_status = True
        line_ctr = 1
        for line in opened_file:
            for validator_pattern in VALIDATOR_PATTERNS:
                # Check for patterns one at a time
                if validator_pattern.search(line):
                    if file_status:
                        LOG.info("Invalid pattern -- " +
                                 validator_pattern.pattern +
                                 " -- found in : " + file_path
                                )
                    LOG.info("Line %d: %s", line_ctr, line.strip())
                    file_status = False
            line_ctr += 1

    return file_status


def check_namespaces(file_path):
    """Scans namespace openings and closings."""
    # only check for src files
    if not file_path.startswith(DEFAULT_DIRS[0]):
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
        matches = re.findall(
            r'^ *namespace ([a-z_-]+) {$|^ *} +\/\/ namespace ([a-z_-]+)$',
            data,
            flags=re.MULTILINE
        )

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
                    namespace_errors.append(
                        "This namespace was closed in wrong order: '" +
                        match[1] +
                        "' -- in " + file_path
                    )
                # check if present at all
                if not match[1] in open_namespaces:
                    namespace_errors.append(
                        "This namespace was closed, but is missing a correct "
                        "opening: '" + match[1] + "' -- in " + file_path
                        )
                else:
                    # remove from open list
                    open_namespaces.remove(match[1])

        if required_namespaces:
            namespace_errors.append(
                "Required namespaces are missing or in wrong order: " +
                str(required_namespaces) + " -- in " + file_path
                )

        if open_namespaces:
            namespace_errors.append(
                "These namespaces were not closed properly: " +
                str(open_namespaces) + " -- in " + file_path
                )

        if namespace_errors:
            LOG.info("Invalid namespace style -- in " + file_path)
            for error in namespace_errors:
                LOG.info("  " + error)
            return False
        return True


def check_includes(file_path):
    """Checks whether local includes are done via #include<...>"""
    with open(file_path, "r") as file:
        path_pattern = re.compile(r'^#include <(include/)?(.*?)>')
        linenum = 0
        file_status = True
        for line in file:
            linenum += 1
            res = path_pattern.match(line)
            if res:
                path = res.groups()[1]
                if path in PATHS:
                    if file_status:
                        LOG.info("Invalid include in %s", file_path)
                    file_status = False
                    LOG.info("Line %s: %s", linenum, line.strip())
        if not file_status:
            LOG.info('includes for peloton header files have must not have '
                     'brackets'
                    )
    return file_status


VALIDATORS = [
    check_common_patterns,
    check_includes,

    # Uncomment the below validator once the namespace refactoring is done
    #check_namespaces,

    # Uncomment the below validator when all files are clang-format-compliant
    #check_format
]


def validate_file(file_path):
    """Validates the source file that is passed as argument."""
    if not file_path.endswith(".h") and not file_path.endswith(".cpp"):
        return True

    file_status = True
    for validator in VALIDATORS:
        if not validator(file_path):
            file_status = False

    return file_status


def validate_dir(dir_path):
    """Validates all the files in the directory passed as argument."""
    dir_status = True
    for subdir, _, files in os.walk(dir_path):  # _ represents directories.
        for file in files:
            file_path = subdir + os.path.sep + file

            if not validate_file(file_path):
                dir_status = False
    return dir_status

            #END IF
        #END FOR [file]
    #END FOR [os.walk]
#END VALIDATE_DIR(DIR_PATH)


## ==============================================
##                 Main Function
## ==============================================

if __name__ == '__main__':

    PARSER = argparse.ArgumentParser(
        description='Perform source code validation on Peloton source'
        )
    PARSER.add_argument("-f", "--files", nargs='*',
                        help="A list of files to validate"
                       )
    ARGS = PARSER.parse_args()

    LOG.info("Running source validator ...")
    LOG.info("Peloton root : " + PELOTON_DIR)


    if ARGS.files:
        # Validate just the provided files.

        # In this mode, we perform explicit clang-format checks
        VALIDATORS.append(clang_check)
        for each_file in ARGS.files:
            each_file = os.path.abspath(each_file.lower())

            # Fail if the file isn't really a file
            if not os.path.isfile(each_file):
                LOG.info("ERROR: " + each_file + " isn't a file")
                sys.exit(EXIT_FAILURE)

            # Skip files not in 'src' or 'test'
            if not each_file.startswith(DEFAULT_DIRS[0]) and \
                not each_file.startswith(DEFAULT_DIRS[1]):
                LOG.info("Skipping non-Peloton source : " + each_file)
                continue

            # Looks good, let's validate
            LOG.info("Scanning file : " + each_file)
            status = validate_file(each_file)
            if not status:
                LOG.info("Validation NOT successful")
                sys.exit(EXIT_FAILURE)
        #END FOR

    else:
        ## Validate all files in source and test directories
        for directory in DEFAULT_DIRS:
            LOG.info("Scanning directory : " + directory)

            status = validate_dir(directory)
            if not status:
                LOG.info("Validation NOT successful")
                sys.exit(EXIT_FAILURE)
        #END FOR

    LOG.info("Validation successful")
    sys.exit(EXIT_SUCCESS)
