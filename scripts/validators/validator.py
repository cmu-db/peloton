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
PELOTON_SRC_BACKEND_DIR = os.path.join(PELOTON_SRC_DIR, "backend")
PELOTON_TESTS_DIR = os.path.join(PELOTON_DIR, "tests")

# DEFAULT DIRS
DEFAULT_DIRS = []
DEFAULT_DIRS.append(PELOTON_SRC_BACKEND_DIR)
DEFAULT_DIRS.append(PELOTON_TESTS_DIR)

EXIT_SUCCESS = 0
EXIT_FAILURE = -1

VALIDATOR_PATTERNS = ["std\:\:cout", " printf\(", "cout", " malloc\(", " free\("]

## ==============================================
## 			LOGGING CONFIGURATION
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
## 		  UTILITY FUNCTION DEFINITIONS
## ==============================================

#validate the file passed as argument
def validate_file(file_path):

	file_name = os.path.basename(file_path)
	abs_path = os.path.abspath(file_path)
	rel_path_from_peloton_dir = os.path.relpath(abs_path,PELOTON_DIR)

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
## 				Main Function
## ==============================================

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description='Check for some strings')

	args = parser.parse_args()

	for dir in DEFAULT_DIRS:
		LOG.info("Scanning : " + dir)

		status = validate_dir(dir)
		if status == False:
			LOG.info("Validation not successful")
			sys.exit(EXIT_FAILURE)

	LOG.info("Validation successful")
	sys.exit(EXIT_SUCCESS)
