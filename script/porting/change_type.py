#!/usr/bin/env python

import os
import sys
import re

regex = re.compile("(.*_out)(.*)(\(.*\<const\s)(List)(\s\*.*;.*)")
found = 0
lineNum = 0
filePath = "src/postgres/backend/nodes/outfuncs.cpp"

with open(filePath, "r") as fd1:
	lines = fd1.readlines()
fd1.close()

with open(filePath, "r") as fd:
	for line in fd:
		m = regex.match(line)
		if m is None:
			lineNum += 1
			continue
	
		underscore_out = m.group(1)
		replacement_str = m.group(2)
		parenth_to_space = m.group(3)
		str_to_be_replaced = m.group(4)
		space_to_semicolon = m.group(5)
	
		print "lineNum: ", lineNum
	
		#print "underscore_out: ", underscore_out
		print "replacement_str: ", replacement_str
		#print "parenth_to_space: ", parenth_to_space
		print "str_to_be_replaced: ", str_to_be_replaced
		#print "space_to_semicolon: ", space_to_semicolon
	
		old_str = underscore_out + replacement_str + parenth_to_space + str_to_be_replaced + space_to_semicolon
		new_str = underscore_out + replacement_str + parenth_to_space + replacement_str + space_to_semicolon
		
		print old_str
		print new_str
		
		lines[lineNum] = new_str
	
		found += 1
		lineNum += 1
	# END FOR

fd.close()
print found

with open(filePath,"w") as fd2:
	fd2.seek(0,0)
	fd2.writelines(lines)
fd2.close()
