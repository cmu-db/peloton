mkdir -p lib
mkdir -p out
wget -nc -P lib https://jdbc.postgresql.org/download/postgresql-9.4.1209.jre6.jar
javac -classpath ./lib/postgresql-9.4.1209.jre6.jar:./lib/ttjdbc7.jar -d out src/PelotonTest.java
jar -cvf out.jar -C out .
java -cp out.jar:./lib/postgresql-9.4.1209.jre6.jar:./lib/ttjdbc7.jar PelotonTest $1
