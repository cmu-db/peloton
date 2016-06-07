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

      ASSERT_TRUE(sqlParser->parse(*sqlString));

      std::string parseResult;
      sqlParser->getStatement()->toString(parseResult);

      int compareResult = parseResult.compare(*sqlString);
      ASSERT_EQ(compareResult, 0);

      if (compareResult == 0) {
        LOG_INFO("O : %s", parseResult.c_str());
        std::string buf;
        LOG_INFO("String: %s", sqlParser->getStatement()->toTreeString(buf).c_str());
      }

      sqlString++;
    }

  };

};

TEST_F(ParserTest, CloneInt) {

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

}  // End test namespace
}  // End peloton namespace
