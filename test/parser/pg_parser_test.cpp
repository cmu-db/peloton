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
#include "common/logger.h"
#include "parser/pg_parser.h"
#include "parser/parser.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Parser Tests
//===--------------------------------------------------------------------===//

class PgParserTests : public PelotonTest {};

TEST_F(PgParserTests, BasicTest) {
  std::vector<std::string> queries;

  // SELECT statement
  // Simple select
  queries.push_back("SELECT * FROM foo;");
  // Select with functional call
  // queries.push_back("SELECT COUNT(*) FROM foo;");
  // Select constants
  // queries.push_back("SELECT 'str', 1 FROM foo;");
  // Select for update
  // queries.push_back("SELECT * FROM db.orders FOR UPDATE;");
  // Select distinct
  // queries.push_back("SELECT COUNT(DISTINCT foo.name) FROM foo;");
  // Select with join
  // queries.push_back("SELECT * FROM foo INNER JOIN bar ON foo.id=bar.id;");
  // Select with nested query
  // queries.push_back("SELECT * FROM (SELECT * FROM foo) as t, bar;");
  // Select from multiple tables
  // queries.push_back("SELECT foo.name FROM foo, bar WHERE foo.id = bar.id;");
  // Select with complicated where
  // queries.push_back("SELECT * FROM foo WHERE id > 3 AND value < 10 OR id < 3 AND value > 10;");


  auto parser = pgparser::Pg_Parser::GetInstance();
  // auto ref_parser = parser::Parser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    // auto ref_stmt_list = ref_parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    // LOG_INFO("%d : %s", ++ii, ref_stmt_list->GetInfo().c_str());
    delete stmt_list;
    // delete ref_stmt_list;
  }
}


}  // End test namespace
}  // End peloton namespace
