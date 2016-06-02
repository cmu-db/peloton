mkdir -p lib
mkdir -p out
wget -nc -P lib https://jdbc.postgresql.org/download/postgresql-9.4-1201.jdbc4.jar
javac -classpath ./lib/postgresql-9.4-1201.jdbc4.jar -d out src/PelotonTest.java
jar -cvf out.jar -C out .
java -cp out.jar:./lib/postgresql-9.4-1201.jdbc4.jar PelotonTest
