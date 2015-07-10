#!/usr/bin/env python

import os
import sys
import re
#import pprint;

found = 0
fixed = 0
unfixed = 0
warning_log = "warning-log.txt"
regex = re.compile(".*\/[^src]*(src\/postgres\/.*.cpp):([\d]+):[\d]+: note: in expansion of macro '([^']+)'")
regex_macro = re.compile("l.*")

with open(warning_log, "r") as fd:
	for line in fd:
		m = regex.match(line)
		if m is None: continue

		filePath = m.group(1)
		lineNum = int(m.group(2))
		macroName = m.group(3)
		
		m_macro = regex_macro.match(macroName)
		if m_macro is None: continue
		
		#print "filePath: ", filePath
		#print "lineNum: ", lineNum
		#print "macroName: ", macroName
		
		found += 1
		
		with open(filePath, "r+") as fd2:
			lines = fd2.readlines()
			criticalLine = lines[lineNum - 1]
			#print "criticalLine: ", criticalLine

			regex2 = re.compile("([^=]*)(=\s)(l[^\(]*)(\([^\)]*\))(;[\n]*[\t]*)")
			m2 = regex2.match(criticalLine)
			
			if m2 is None: unfixed += 1
			else:
				print "criticalLine: ", criticalLine
				lvalue = m2.group(1)
				placeholder_2 = m2.group(2)
				rvalue_macro = m2.group(3)
				placeholder_4 = m2.group(4)
				placeholder_5 = m2.group(5)
				
				#print "lvalue: ", lvalue
				#print "rvalue_macro: ", rvalue_macro
				#print ""
				
				split_array = lvalue.split()
				
				#pprint.pprint(split_array);
				
				cast_type_base = split_array[0]
				cast_var_name = split_array[1]
				
				regex3 = re.compile("(\**)[^*]+")
				m3 = regex3.match(cast_var_name)
				
				cast_type_ptr = m3.group(1)
				
				if cast_type_ptr != '':
					cast_type = cast_type_base + " " + cast_type_ptr
				else:
					cast_type = cast_type_base
				
				#print "cast_type_base: ", cast_type_base
				#print "cast_var_name: ", cast_var_name
				#print "cast_type: ", cast_type
				
				replacementLine = lvalue + placeholder_2 + "static_cast<" + cast_type + ">(" + rvalue_macro + placeholder_4 + ")" + placeholder_5
				print "replacementLine: ", replacementLine
				
				lines[lineNum - 1] = replacementLine
				
				fd2.seek(0,0)
				fd2.writelines(lines)
				
				fd2.seek(0,0)
				newLine = fd2.readlines()[lineNum - 1]
				print "newLine: ", newLine
				
				fixed += 1
			#END IF
		#END WITH
		fd2.close()
		
	#END FOR
#END WITH
fd.close()

print "found: ", found
print "fixed: ", fixed
print "unfixed: ", unfixed