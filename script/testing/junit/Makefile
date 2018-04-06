


.SUFFIXES: .java .class

CP = .:lib/junit-4.12.jar:lib/hamcrest-core-1.3.jar:lib/postgresql-9.4.1209.jre6.jar
JC = javac
JFLAGS = -g
JUNIT = org.junit.runner.JUnitCore
RM = rm

.java.class:
	$(JC) $(JFLAGS) -cp $(CP) $*.java

CLASSES = \
	PLTestBase.java \
	InsertTPCCTest.java \
	InsertPSTest.java \
	UpdateTest.java

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class

test:	$(CLASSES)
	java -jar lib/junit-platform-console-standalone-1.1.0.jar \
	  --class-path $(CP) \
	  -c InsertTPCCTest \
	  -c InsertPSTest \
          -c UpdateTest




