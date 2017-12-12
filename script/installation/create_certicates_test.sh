#!/bin/bash

cd ../../data
export DATA_DIR=$(pwd)
echo $DATA_DIR
mkdir certs crl newcerts private
chmod 700 private
touch index.txt
echo 1000 > serial

# create root key
openssl genrsa -out $DATA_DIR/private/ca.key.pem 4096
chmod 400 private/ca.key.pem

# Create the root certificate
# -subj option automatically fill the info.
echo $DATA_DIR
openssl req -subj /CN=root/ -key private/ca.key.pem -new -x509 -days 7300 -extensions v3_ca -out certs/ca.cert.pem
chmod 444 certs/ca.cert.pem

# Verify the root certificate
openssl x509 -noout -text -in certs/ca.cert.pem

# Create the intermediate pair
mkdir intermediate
cd intermediate
mkdir certs crl csr newcerts private
cp $DATA_DIR/intermediate_openssl.cnf $DATA_DIR/intermediate/openssl.cnf
chmod 700 private
touch index.txt
echo 1000 > serial
echo $(pwd)
echo 1000 > crlnumber

# create the intermediate key
cd $DATA_DIR
openssl genrsa -out intermediate/private/intermediate.key.pem 4096
chmod 400 intermediate/private/intermediate.key.pem

# create the intermediate certificate
openssl req -config intermediate/openssl.cnf -subj /CN=intermediate/ -new -key intermediate/private/intermediate.key.pem -out intermediate/csr/intermediate.csr.pem
# -batch avoids questions pop up
openssl ca -config openssl.cnf -extensions v3_intermediate_ca -days 3650 -notext -md sha256 -in intermediate/csr/intermediate.csr.pem -out intermediate/certs/intermediate.cert.pem -batch
chmod 444 intermediate/certs/intermediate.cert.pem

# verify the intermediate certificate
openssl x509 -noout -text -in intermediate/certs/intermediate.cert.pem
openssl verify -CAfile certs/ca.cert.pem intermediate/certs/intermediate.cert.pem

# create the certificate chain file
cat intermediate/certs/intermediate.cert.pem certs/ca.cert.pem > intermediate/certs/ca-chain.cert.pem
chmod 444 intermediate/certs/ca-chain.cert.pem

# create server's key and certificate and sign it
cd $DATA_DIR
openssl genrsa -out intermediate/private/server.key.pem 2048
openssl req -config openssl.cnf -key intermediate/private/server.key.pem -new -sha256 -out intermediate/csr/server.csr.pem -subj /CN=server/
openssl ca -config openssl.cnf -extensions server_cert -days 375 -notext -md sha256 -in intermediate/csr/server.csr.pem -out intermediate/certs/server.cert.pem -batch
chmod 444 intermediate/certs/server.cert.pem
openssl x509 -noout -text -in intermediate/certs/server.cert.pem

# create client's key and certificate and sign it
openssl genrsa -out intermediate/private/client.key.pem 2048
openssl req -config openssl.cnf -key intermediate/private/client.key.pem -new -sha256 -out intermediate/csr/client.csr.pem -subj /CN=client/
openssl ca -config openssl.cnf -extensions usr_cert -days 375 -notext -md sha256 -in intermediate/csr/client.csr.pem -out intermediate/certs/client.cert.pem -batch
chmod 444 intermediate/certs/client.cert.pem
openssl x509 -noout -text -in intermediate/certs/client.cert.pem


