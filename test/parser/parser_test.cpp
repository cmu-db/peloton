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
#include "parser/parser/pg_query.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Parser Tests
//===--------------------------------------------------------------------===//

class ParserTest : public PelotonTest {};

void ParseSQLStrings(const std::vector<std::string>& sql_strings,
                     const bool expect_failure){

  // Initialize the postgres memory context
  pg_query_init();

  for(auto sql_string : sql_strings) {
    PgQueryParseResult result = pg_query_parse(sql_string.c_str());

    if(expect_failure == true){
      EXPECT_NE(result.error, nullptr);
      LOG_INFO("input: %s", sql_string.c_str());
      LOG_ERROR("error: %s at %d", result.error->message, result.error->cursorpos);
    }
    else {
      EXPECT_EQ(result.error, nullptr);
      //LOG_INFO("parse tree: %s", result.parse_tree);
    }

    pg_query_free_parse_result(result);
  }

}

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

  bool expect_failure = false;
  ParseSQLStrings(sqlStrings, expect_failure);
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

  bool expect_failure = false;
  ParseSQLStrings(sqlStrings, expect_failure);
}

TEST_F(ParserTest, DDLTest) {

  std::vector<std::string> sqlStrings;

  sqlStrings.push_back("CREATE TABLE Foo(ID integer, SALARY integer)");
  sqlStrings.push_back("DROP TABLE Foo");
  sqlStrings.push_back("CREATE INDEX FooIndex ON FOO(ID)");
  sqlStrings.push_back("DROP INDEX FooIndex");

  bool expect_failure = false;
  ParseSQLStrings(sqlStrings, expect_failure);
}


TEST_F(ParserTest, OrderByTest) {

  std::vector<std::string> sqlStrings;

  sqlStrings.push_back("SELECT id FROM a.b ORDER BY id");
  sqlStrings.push_back("SELECT 1 FROM a WHERE 1 = 1 AND 1");

  bool expect_failure = false;
  ParseSQLStrings(sqlStrings, expect_failure);
}

TEST_F(ParserTest, MoreTest) {

  std::vector<std::string> sqlStrings;

  sqlStrings.push_back("SELECT 1");
  sqlStrings.push_back("SELECT * FROM x WHERE z = 2");
  sqlStrings.push_back("SELECT 5.41414");
  sqlStrings.push_back("SELECT $1");
  sqlStrings.push_back("SELECT 999999999999999999999::numeric/1000000000000000000000");
  sqlStrings.push_back("SELECT 4790999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");

  bool expect_failure = false;
  ParseSQLStrings(sqlStrings, expect_failure);
}

TEST_F(ParserTest, FailureTest) {

  std::vector<std::string> sqlStrings;

  sqlStrings.push_back("SELECT ?");

  bool expect_failure = true;
  ParseSQLStrings(sqlStrings, expect_failure);
}

}  // End test namespace
}  // End peloton namespace
