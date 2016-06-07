//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser_test.cpp
//
// Identification: test/common/parser_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <vector>

#include "common/harness.h"

#include "parser/SQL92Parser.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Parser Tests
//===--------------------------------------------------------------------===//

class ParserTest : public PelotonTest {};


class SqlEngineTestCase {

  parser::SQLParser *sqlParser = nullptr;

 public:


  SqlEngineTestCase(parser::SQLParser *sqlParser){
    this->sqlParser = sqlParser;
  }

  virtual ~SqlEngineTestCase(){
  }

  void parse(std::vector<std::string> &sqlStrings){
    std::vector<std::string>::iterator sqlString = sqlStrings.begin();
    while(sqlString != sqlStrings.end()) {

      LOG_INFO("I : %s", (*sqlString).c_str());

      sqlParser->parse(*sqlString);

      std::string parseResult;
      sqlParser->getStatement()->toString(parseResult);

      int compareResult = parseResult.compare(*sqlString);

      if (compareResult == 0) {
        std::string buf;
        LOG_INFO("String: %s", sqlParser->getStatement()->toTreeString(buf).c_str());
      }
      else {
        auto error = sqlParser->getError();
        auto error_msg = error->getMessage();
        LOG_INFO("Error: %s", error_msg.c_str());
      }

      sqlString++;
    }

  };

};

TEST_F(ParserTest, SQL92Test) {

  std::vector<std::string> sqlStrings;

  sqlStrings.push_back("SELECT * FROM SAMPLE1");
  sqlStrings.push_back("SELECT * FROM SAMPLE2 WHERE A = B");
  sqlStrings.push_back("SELECT * FROM SAMPLE3 LIMIT 10");

  sqlStrings.push_back("INSERT INTO abc VALUES (1234)");
  sqlStrings.push_back("INSERT INTO abc (age) VALUES (34)");
  sqlStrings.push_back("INSERT INTO abc (name,age) VALUES (skonno,34)");

  sqlStrings.push_back("DELETE FROM abc");
  sqlStrings.push_back("DELETE FROM abc WHERE A = B");

  sqlStrings.push_back("UPDATE abc SET age = 34");
  sqlStrings.push_back("UPDATE abc SET age = 34 WHERE name = skonno");

  parser::SQL92Parser sqlParser;
  SqlEngineTestCase sqlTestCase(&sqlParser);
  sqlTestCase.parse(sqlStrings);
}

TEST_F(ParserTest, ExpressionTest) {

  std::vector<std::string> sqlStrings;

  sqlStrings.push_back("SELECT * FROM A WHERE age == 18");
  sqlStrings.push_back("SELECT * FROM A WHERE age = 18");
  sqlStrings.push_back("SELECT * FROM A WHERE age < 18");
  sqlStrings.push_back("SELECT * FROM A WHERE age <= 18");
  sqlStrings.push_back("SELECT * FROM A WHERE age > 18");
  sqlStrings.push_back("SELECT * FROM A WHERE age >= 18");
  sqlStrings.push_back("SELECT * FROM A WHERE age != 18");

  sqlStrings.push_back("SELECT * FROM Person WHERE age >= 18 AND age <= 35");
  sqlStrings.push_back("SELECT * FROM Person WHERE age >= 18 OR age <= 35");

  sqlStrings.push_back("SELECT * FROM Person WHERE age >= 18 AND age <= 35 AND age <= 10");
  sqlStrings.push_back("SELECT * FROM Person WHERE age >= 18 OR age <= 35 OR age <= 10");
  sqlStrings.push_back("SELECT * FROM Person WHERE age >= 18 AND age <= 35 OR age <= 10");
  sqlStrings.push_back("SELECT * FROM Person WHERE age >= 18 OR age <= 35 AND age <= 10");

  sqlStrings.push_back("SELECT * FROM Person WHERE age >= 18 AND age <= 35 AND age <= 10 AND age >= 5");
  sqlStrings.push_back("SELECT * FROM Person WHERE age >= 18 OR age <= 35 OR age <= 10 OR age >= 5");

  parser::SQL92Parser sqlParser;
  SqlEngineTestCase sqlTestCase(&sqlParser);
  sqlTestCase.parse(sqlStrings);
}

TEST_F(ParserTest, DDLTest) {

  std::vector<std::string> sqlStrings;

  sqlStrings.push_back("CREATE TABLE Foo(ID integer, SALARY integer)");
  sqlStrings.push_back("DROP TABLE Foo");
  sqlStrings.push_back("CREATE INDEX FooIndex ON FOO(ID)");
  sqlStrings.push_back("DROP INDEX FooIndex");

  parser::SQL92Parser sqlParser;
  SqlEngineTestCase sqlTestCase(&sqlParser);
  sqlTestCase.parse(sqlStrings);
}


TEST_F(ParserTest, OrderByTest) {

  std::vector<std::string> sqlStrings;

  //sqlStrings.push_back("SELECT id FROM a.b ORDER BY id");
  //sqlStrings.push_back("SELECT 1 FROM a WHERE 1 = 1 AND 1");

  parser::SQL92Parser sqlParser;
  SqlEngineTestCase sqlTestCase(&sqlParser);
  sqlTestCase.parse(sqlStrings);
}

}  // End test namespace
}  // End peloton namespace
