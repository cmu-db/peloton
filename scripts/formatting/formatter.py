import argparse
import logging
import os
import re
import sys


## ==============================================
## 			MISCELLANEOUS CONFIGURATION
## ==============================================

CODE_SOURCE_DIR = os.path.abspath(os.path.dirname(__file__))

#absolute path to peloton directory is calculated from current directory
#directory structure: peloton/scripts/formatting/<this_file>
#PELOTON_DIR needs to be redefined if the directory structure is changed
PELOTON_DIR = reduce(os.path.join, [CODE_SOURCE_DIR, os.path.pardir, os.path.pardir])

#other directory paths used are relative to peloton_dir
PELOTON_SRC_DIR = os.path.join(PELOTON_DIR, "src")
PELOTON_SRC_BACKEND_DIR = os.path.join(PELOTON_SRC_DIR, "backend")

#header framework, dynamic information will be added inside function
header_comment_line_1 = "/*-----------------------------------------------------------------\n"
header_comment_line_2 = " *\n"
header_comment_line_3 = " * Filename: "
header_comment_line_4 = " *\n"
header_comment_line_5 = " * Identification: "
header_comment_line_6 = " *\n"
header_comment_line_7 = " * Copyright (c) 2015: Carnegie Mellon University Database Group\n"
header_comment_line_8 = " *\n"
header_comment_line_9 = " *-----------------------------------------------------------------\n"
header_comment_line_10 = " */\n\n"

header_comment_1 = header_comment_line_1 + header_comment_line_2
header_comment_3 = header_comment_line_4
header_comment_5 = header_comment_line_6 + header_comment_line_7 + header_comment_line_8 \
						+ header_comment_line_9 + header_comment_line_10

#regular expresseion used to track header
header_regex = re.compile("(\/\*-*[^-]*-*\n[\s]*\*\/[\n]*)(#include|#pragma)")


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

#add suitable header to the file passed as argument
def add_header_file(file_path):
	
	file_name = os.path.basename(file_path)
	abs_path = os.path.abspath(file_path)
	rel_path_from_peloton_dir = os.path.relpath(abs_path,PELOTON_DIR)
	
	header_comment_2 = line_3 + file_name + "\n"
	header_comment_4 = line_5 + rel_path_from_peloton_dir + "\n"
	header_comment = header_comment_1 + header_comment_2 + header_comment_3 \
						+ header_comment_4 + header_comment_5
	print header_comment
	
	with open(file_path, "r+") as fd:
		file_data = fd.read()
		new_file_data = header_comment + file_data
		
		fd.seek(0,0)
		fd.truncate()
		fd.write(new_file_data)
	#END WITH
	
	fd.close()
#END ADD_HEADER_FILE(FILE_NAME)


#add suitable headers to all the files in the directory passed as argument
def add_headers_dir(dir_path):
	
	for subdir, dirs, files in os.walk(dir_path):
		for file in files:
			#print os.path.join(subdir, file)
			file_path = subdir + os.path.sep + file
			
			if file_path.endswith(".h") or file_path.endswith(".cpp"):
				abs_path = os.path.abspath(file_path)
				rel_path_from_peloton_dir = os.path.relpath(abs_path,PELOTON_DIR)
				
				header_comment_2 = header_comment_line_3 + file + "\n"
				header_comment_4 = header_comment_line_5 + rel_path_from_peloton_dir + "\n"
				
				header_comment = header_comment_1 + header_comment_2 + header_comment_3 \
									+ header_comment_4 + header_comment_5
				
				with open(file_path, "r+") as fd:
					file_data = fd.read()
					new_file_data = header_comment + file_data
					
					fd.seek(0,0)
					fd.truncate()
					fd.write(new_file_data)
				#END WITH
				
				fd.close()
			#END IF
		#END FOR [file]
	#END FOR [os.walk]
#END ADD_HEADERS_DIR(DIR_PATH)


def strip_header_file(file_path):
	
	with open(file_path, "r+") as fd:
		
		file_data = fd.read()
		
		header_match = header_regex.match(file_data)
		if header_match is None: 
			return
		
		header_comment = header_match.group(1)
		
		new_file_data = file_data.replace(header_comment,"")
		
		fd.seek(0,0)
		fd.truncate()
		fd.write(new_file_data)
	#END WITH
	
	fd.close()
#END STRIP_HEADER_FILE(FILE_PATH)


def strip_headers_dir(dir_path):
	
	for subdir, dirs, files in os.walk(dir_path):
		for file in files:
			file_path = subdir + os.path.sep + file
			
			if file_path.endswith(".h") or file_path.endswith(".cpp"):
				with open(file_path, "r+") as fd:
					file_data = fd.read()
					
					header_match = header_regex.match(file_data)
					if header_match is None:
						LOG.info("regex didn't match for file: " + file)
						continue
					
					header_comment = header_match.group(1)
					
					new_file_data = file_data.replace(header_comment,"")
					
					fd.seek(0,0)
					fd.truncate()
					fd.write(new_file_data)
				#END WITH
				
				fd.close()
			#END IF
		#END FOR [file]
	#END FOR [os.walk]
#END STRIP_HEADERS_DIR(DIR_PATH)


def clang_format_file(file_path):
	formatting_command = "clang-format-3.6 -i " + file_path
	LOG.info(formatting_command)
	os.system(formatting_command)
#END CLANG_FORMAT_FILE(FILE_PATH)


def clang_format_dir(dir_path):
	for subdir, dirs, files in os.walk(dir_path):
		for file in files:
			file_path = subdir + os.path.sep + file
			
			if file_path.endswith(".h") or file_path.endswith(".cpp"):
				formatting_command = "clang-format-3.6 -i " + file_path
				LOG.info(formatting_command)
				os.system(formatting_command)
			#END IF
		#END FOR [file]
	#END FOR [os.walk]
#END CLANG_FORMAT_DIR(DIR_PATH)


## ==============================================
## 				Main Function
## ==============================================

if __name__ == '__main__':
	
	parser = argparse.ArgumentParser(description='Add/delete headers and/or format source code')
	
	parser.add_argument("-a", "--add-header", help='add suitable header(s)', action='store_true')
	parser.add_argument("-s", "--strip-header", help='strip existing header(s)', action='store_true')
	parser.add_argument("-c", "--clang-format-code", help='clang-format source code', action='store_true')
	parser.add_argument("-f", "--file-name", help='file to be acted on', nargs=1)
	parser.add_argument("-d", "--dir-name", help='directory containing files to be acted on', nargs=1)
	
	args = parser.parse_args()
	
	if args.add_header and args.strip_header:
		LOG.error("adding and stripping headers cannot be done together -- exiting")
		sys.exit("adding and stripping headers cannot be done together")
	if args.file_name and args.dir_name:
		LOG.error("file_name and dir_name cannot be specified together -- exiting")
		sys.exit("file_name and dir_name cannot be specified together")
	
	
	if args.add_header:
		LOG.info("You want to add headers")
		
		if args.file_name:
			add_header_file(''.join(args.file_name))
		elif args.dir_name:
			print (''.join(args.dir_name))
			add_headers_dir(''.join(args.dir_name))
		else:
			add_headers_dir(PELOTON_SRC_BACKEND_DIR)
	#END IF ADD_HEADER
	
	
	if args.strip_header:
		LOG.info("You want to strip headers")
		
		if args.file_name:
			strip_header_file(''.join(args.file_name))
		elif args.dir_name:
			strip_headers_dir(''.join(args.dir_name))
		else:
			strip_headers_dir(PELOTON_SRC_BACKEND_DIR)
	#END IF STRIP_HEADER
	
	
	if args.clang_format_code:
		LOG.info("You want to clang-format source code")
		
		if args.file_name:
			clang_format_file(''.join(args.file_name))
		elif args.dir_name:
			clang_format_dir(''.join(args.dir_name))
		else:
			clang_format_dir(PELOTON_SRC_BACKEND_DIR)
	#END IF CLANG_FORMAT
	
	
	if args.file_name:
		LOG.info("You want to work on the file: " + ''.join(args.file_name))
	if args.dir_name:
		LOG.info("You want to work on the files in the directory " + ''.join(args.dir_name))	