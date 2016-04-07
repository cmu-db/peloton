export PELOTON_HOME=/home/vagrant/peloton
echo $PELOTON_HOME
g++ -I$PELOTON_HOME/src/postgres/include -I$PELOTON_HOME/src/postgres/backend -std=c++11 -fpic -c sample_udf_test.cpp -o sample_udf.o
g++ -shared -o sample_udf.so sample_udf.o

g++ -I$PELOTON_HOME/src/postgres/include -I$PELOTON_HOME/src/postgres/backend -std=c++11 -fpic -c spi_udf_test.cpp -o spi_udf.o
g++ -shared -o spi_udf.so spi_udf.o

rm -rf sample_udf.o 
rm -rf spi_udf.o
