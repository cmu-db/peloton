cd peloton/data/cert
openssl x509 -in ca.cert.pem -inform pem -out ca.der -outform der
keytool -v -printcert -file ca.der
keytool -importcert -alias startssltmp -keystore $JAVA_HOME/jre/lib/security/cacerts -storepass changeit -file ca.der

