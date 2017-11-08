import os
with open("out.txt") as fp:  
   for line in fp.readline():
     call("python script/formatting/formatter.py -c "+line.split()[1], shell=True)
       
