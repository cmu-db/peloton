export PELOTON_HOME=/home/vagrant/peloton
echo "PELOTON_HOME = $PELOTON_HOME"

echo "compiling sample udf..."
g++ -I$PELOTON_HOME/src/postgres/include -I$PELOTON_HOME/src/postgres/backend -std=c++11 -fpic -c sample_udf_test.cpp -o sample_udf.o
g++ -shared -o sample_udf.so sample_udf.o

echo "compile spi udf..."
g++ -I$PELOTON_HOME/src/postgres/include -I$PELOTON_HOME/src/postgres/backend -std=c++11 -fpic -c spi_udf_test.cpp -o spi_udf.o
g++ -shared -o spi_udf.so spi_udf.o

echo "move so files to /usr/local/lib"
sudo mv sample_udf.so /usr/local/lib/sample_udf.so
sudo mv spi_udf.so /usr/local/lib/spi_udf.so

echo "remove leftover files"
rm -rf sample_udf.o sample_udf.so spi_udf.o spi_udf.so

echo "done."
