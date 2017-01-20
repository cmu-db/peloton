#!/bin/sh

TABLE=$1
NAME=$2
TYPE=$3
KEYS=$4

echo "insert into information_schema_indexes VALUES ('$TABLE', '$NAME', '$TYPE', '$KEYS');" | psql "sslmode=disable" -U postgres -h localhost -p 15721
