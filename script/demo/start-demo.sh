#!/bin/sh

echo "load stats from '/home/pavlo/samples.txt';" | psql "sslmode=disable" -U postgres -h localhost -p 15721
