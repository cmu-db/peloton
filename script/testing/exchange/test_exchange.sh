#!/bin/bash
num_rows=10
if [ ! -z "$1" ]; then
    num_rows=$1
fi
mkdir -p lib
mkdir -p out
wget -nc -P lib https://jdbc.postgresql.org/download/postgresql-9.4.1209.jre6.jar
wget -nc -P lib http://central.maven.org/maven2/commons-cli/commons-cli/1.3.1/commons-cli-1.3.1.jar
javac -classpath ./lib/postgresql-9.4.1209.jre6.jar:./lib/commons-cli-1.3.1.jar -d out src/ExchangeTest.java
jar -cvf out.jar -C out .
java -cp out.jar:./lib/postgresql-9.4.1209.jre6.jar:./lib/commons-cli-1.3.1.jar ExchangeTest "$@"
