#!/usr/bin/env python

import os
import sys
import re

regex1 = re.compile("((?:.*)\.(?cpp)):([\d]+):([\d]+): warning: invalid conversion from \'(.*?)\' to '(.*?)' \[\-fpermissive\]")

found = 0
unfixed = 0
fixed = 0
searchStr = "postgres"
filePath_to_pg = "src/postgres"

with open(sys.argv[1], "r+") as fd1:
	for line in fd1:
		m1 = regex1.match(line)
		if m1 is None: continue

		fileName = m1.group(1)
		lineNum = int(m1.group(2))
		offset = int(m1.group(3))
		targetType = m1.group(5)

		if targetType.find("{aka") != -1:
			targetType = targetType.split(" ")[0]
		#END IF

		regex3 = re.compile("([^*]*)(.*)")
		m3 = regex3.match(targetType)
		if m3:
			targetType_beforespace = m3.group(1)
			targetType_afterspace = m3.group(2)
			targetType = targetType_beforespace + " " + targetType_afterspace

			#print "targetType_beforespace:", targetType_beforespace
			#print "targetType_afterspace:", targetType_afterspace

		#print m1.groups()

		#print "fileName:", fileName
		#print "lineNum:", lineNum
		#print "offset:", offset
		#print "targetType:", targetType

		if fileName.find(searchStr) != -1:
			filePath_from_pg = fileName.split("postgres")[1]
			#print "filePath_from_pg:", filePath_from_pg
			filePath_abs = filePath_to_pg + filePath_from_pg
			#print "filePath_abs:", filePath_abs

			with open(filePath_abs, "r+") as fd2:
				lines = fd2.readlines()
				criticalLine = lines[lineNum - 1]
				print "criticalLine:", criticalLine

				# palloc[\d]?\((.*)\);
				#regex2 = re.compile("(.*)(malloc([\d])?\((.*)\);)")
				regex2 = re.compile("([^=]*\=\s)(.*\(.*\);)")
				m2 = regex2.match(criticalLine);

				if m2 is None:
					unfixed += 1
					#print "didn't find pattern"

				else:
					beforeGroup = m2.group(1)
					fromGroup = m2.group(2)
					#print "beforeGroup:", beforeGroup
					#print "fromGroup:", fromGroup

					# update this part to include static_cast
					substituteStr = "static_cast<" + targetType + ">(" + fromGroup[:-1] + ");"
					#print "substituteStr:", substituteStr

					# update the entire line
					substituteLine = beforeGroup + substituteStr + "\n"
					#print "substituteLine:", substituteLine

					lines[lineNum - 1] = substituteLine

					# write substituteLine in place of existing line in file
					fd2.seek(0,0)
					fd2.writelines(lines)
					fixed += 1

					fd2.seek(0,0)
					newline = fd2.readlines()[lineNum - 1]
					print "newline:", newline

				# END IF-ELSE
			# END WITH
			fd2.close()
		# END IF

		found += 1
		#if found == 1: break

	# END FOR
# END WITH
fd1.close()

print "found:", found
print "fixed:", fixed
print "unfixed:", unfixed

