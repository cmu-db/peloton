#!/bin/bash -x

TARGET=$1
DB_HOST=$2
DB_PORT=$3

JDBC_JAR="postgresql-9.4.1209.jre6.jar"

if [ "$#" -ne 3 ]; then
    echo "Missing required arguments: $0 <TARGET> <HOST> <PORT>"
    exit 1
fi

mkdir -p lib
mkdir -p out

wget -nc -P lib https://jdbc.postgresql.org/download/${JDBC_JAR}

javac -classpath ./lib/${JDBC_JAR} -d out src/*.java
jar -cvf out.jar -C out .
java -cp out.jar:./lib/${JDBC_JAR} ${TARGET} ${DB_HOST} ${DB_PORT}
