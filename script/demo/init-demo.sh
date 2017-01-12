#!/bin/sh

echo "create table information_schema_indexes (table_name VARCHAR(32) PRIMARY KEY, index_name VARCHAR(32) PRIMARY KEY, index_type VARCHAR(32), index_keys VARCHAR(64));" | psql "sslmode=disable" -U postgres -h localhost -p 15721 &> /dev/null
