#!/bin/bash -x

mkdir -p lib
mkdir -p out
wget -nc -P lib https://jdbc.postgresql.org/download/postgresql-9.4.1209.jre6.jar
javac -classpath ./lib/postgresql-9.4.1209.jre6.jar -d out src/*.java
jar -cvf out.jar -C out .
if [ "$#" -ne 2 ]; then
	if [ $1 == "batch" ]; then
		mkdir -p tmp
		java -cp out.jar:./lib/postgresql-9.4.1209.jre6.jar PelotonTest $1 > tmp/$1_out
		dif=`diff --suppress-common-lines --ignore-all-space -y tmp/$1_out $1_ref_out.txt`
		if [ $dif == ""]; then
			echo "$1 passed"
		else
			echo "$1 failed"
		fi
	else
		java -cp out.jar:./lib/postgresql-9.4.1209.jre6.jar PelotonTest $1
	fi 
else
	# Pass the file path for copy test
	java -cp out.jar:./lib/postgresql-9.4.1209.jre6.jar PelotonTest $1 $2
fi
