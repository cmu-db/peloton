//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser_test.cpp
//
// Identification: test/parser/parser_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <string>
#include <vector>

#include "common/harness.h"
#include "common/macros.h"
#include "parser/parser.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Parser Tests
//===--------------------------------------------------------------------===//

class ParserTest : public PelotonTest {};

void ParseSQLStrings(UNUSED_ATTRIBUTE const std::vector<std::string>& sql_strings,
                     UNUSED_ATTRIBUTE const bool expect_failure) {


  std::string query_string = sql_strings[0];
  LOG_INFO("Query String: %s", query_string.c_str());

  // This test case works without the last delete statement.
  // When the delete statement is added, it crashes
  /*auto stmt  = parser::Parser::ParseSQLString(query_string);

  parser::SQLStatement* first_stmt = nullptr;

  for(auto s : stmt->GetStatements()){
    first_stmt = s;
    break;
  }

  EXPECT_EQ(first_stmt->GetType(), STATEMENT_TYPE_DELETE);

  delete first_stmt; // When this line is added, it crashes at AbstractExpression destructor
  */

  // This test case crashes on the return from the BuildParseTree function
  auto& peloton_parser = parser::Parser::GetInstance();
  auto parse_tree = peloton_parser.BuildParseTree(query_string);

  EXPECT_EQ(parse_tree->GetType(), STATEMENT_TYPE_DELETE);

}

TEST_F(ParserTest, SQL92Test) {
  std::vector<std::string> sqlStrings;

//  sqlStrings.push_back("SELECT * FROM SAMPLE1");
//  sqlStrings.push_back("SELECT * FROM SAMPLE2 WHERE A = B");
//  sqlStrings.push_back("SELECT * FROM SAMPLE3 LIMIT 10");
//
//  sqlStrings.push_back("INSERT INTO abc VALUES (1234)");
//  sqlStrings.push_back("INSERT INTO abc (age) VALUES (34)");
//  sqlStrings.push_back("INSERT INTO abc (name,age) VALUES (skonno,34)");
//
//  sqlStrings.push_back("DELETE FROM abc");
  sqlStrings.push_back("DELETE FROM abc WHERE id < 2");

//  sqlStrings.push_back("UPDATE abc SET age = 34");
//  sqlStrings.push_back("UPDATE abc SET age = 34 WHERE name = skonno");

  bool expect_failure = false;
  ParseSQLStrings(sqlStrings, expect_failure);
}
/*
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

  sqlStrings.push_back(
      "SELECT * FROM Person WHERE age >= 18 AND age <= 35 AND age <= 10");
  sqlStrings.push_back(
      "SELECT * FROM Person WHERE age >= 18 OR age <= 35 OR age <= 10");
  sqlStrings.push_back(
      "SELECT * FROM Person WHERE age >= 18 AND age <= 35 OR age <= 10");
  sqlStrings.push_back(
      "SELECT * FROM Person WHERE age >= 18 OR age <= 35 AND age <= 10");

  sqlStrings.push_back(
      "SELECT * FROM Person WHERE age >= 18 AND age <= 35 AND age <= 10 AND "
      "age >= 5");
  sqlStrings.push_back(
      "SELECT * FROM Person WHERE age >= 18 OR age <= 35 OR age <= 10 OR age "
      ">= 5");

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
  sqlStrings.push_back(
      "SELECT 999999999999999999999::numeric/1000000000000000000000");
  sqlStrings.push_back(
      "SELECT "
      "479099999999999999999999999999999999999999999999999999999999999999999999"
      "9999999999999999 * "
      "999999999999999999999999999999999999999999999999999999999999999999999999"
      "9999999999999999");

  bool expect_failure = false;
  ParseSQLStrings(sqlStrings, expect_failure);
}

TEST_F(ParserTest, FailureTest) {
  std::vector<std::string> sqlStrings;

  sqlStrings.push_back("SELECT ?");

  bool expect_failure = true;
  ParseSQLStrings(sqlStrings, expect_failure);
}

TEST_F(ParserTest, DDLTest2) {
  std::vector<std::string> sqlStrings;

  sqlStrings.push_back("DROP TABLE IF EXISTS B");

  bool expect_failure = false;
  ParseSQLStrings(sqlStrings, expect_failure);
}
*/
}  // End test namespace
}  // End peloton namespace
