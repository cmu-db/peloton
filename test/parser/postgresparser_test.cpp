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
#include "parser/postgresparser.h"
#include "parser/parser.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// PostgresParser Tests
//===--------------------------------------------------------------------===//

class PostgresParserTests : public PelotonTest {};

TEST_F(PostgresParserTests, BasicTest) {
  std::vector<std::string> queries;

  // Simple select
  queries.push_back("SELECT * FROM foo;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, AggTest) {
  std::vector<std::string> queries;

  // Select with functional call
  queries.push_back("SELECT COUNT(*) FROM foo;");
  queries.push_back("SELECT COUNT(DISTINCT id) FROM foo;");
  queries.push_back("SELECT MAX(*) FROM foo;");
  queries.push_back("SELECT MIN(*) FROM foo;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, GroupByTest) {
  std::vector<std::string> queries;

  // Select with group by clause
  queries.push_back("SELECT * FROM foo GROUP BY id, name HAVING id > 10;");

  auto parser = parser::PostgresParser::GetInstance();

  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();

    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());

    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, OrderByTest) {
  std::vector<std::string> queries;

  // Select with order by clause
  queries.push_back("SELECT * FROM foo ORDER BY id;");
  queries.push_back("SELECT * FROM foo ORDER BY id ASC;");
  queries.push_back("SELECT * FROM foo ORDER BY id DESC;");
  // queries.push_back("SELECT * FROM foo ORDER BY id, name;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, ConstTest) {
  std::vector<std::string> queries;

  // Select constants
  queries.push_back("SELECT 'str', 1 FROM foo;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, JoinTest) {
  std::vector<std::string> queries;

  // Select with join
  queries.push_back("SELECT * FROM foo INNER JOIN bar ON foo.id=bar.id AND foo.val > bar.val;");
  queries.push_back("SELECT * FROM foo LEFT JOIN bar ON foo.id=bar.id;");
  queries.push_back("SELECT * FROM foo RIGHT JOIN bar ON foo.id=bar.id AND foo.val > bar.val;");
  queries.push_back("SELECT * FROM foo FULL OUTER JOIN bar ON foo.id=bar.id AND foo.val > bar.val;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, NestedQueryTest) {
  std::vector<std::string> queries;

  // Select with nested query
  queries.push_back("SELECT * FROM (SELECT * FROM foo) as t;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, MultiTableTest) {
  std::vector<std::string> queries;

  // Select from multiple tables
  queries.push_back("SELECT foo.name FROM (SELECT * FROM bar) as b, foo, bar WHERE foo.id = b.id;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, ExprTest) {
  std::vector<std::string> queries;

  // Select with complicated where, tests both BoolExpr and AExpr
  queries.push_back("SELECT * FROM foo WHERE id > 3 AND value < 10 OR id < 3 AND value > 10;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, DeleteTest) {
  std::vector<std::string> queries;

  // Simple delete
  queries.push_back("DELETE FROM foo;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }

    EXPECT_TRUE(stmt_list->GetNumStatements() == 1);
    EXPECT_TRUE(stmt_list->GetStatement(0)->GetType() == StatementType::DELETE);
    auto delstmt = (parser::DeleteStatement *)stmt_list->GetStatement(0);
    EXPECT_EQ(delstmt->GetTableName(), "foo");
    EXPECT_TRUE(delstmt->expr == nullptr);

    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, DeleteTestWithPredicate) {
  std::vector<std::string> queries;

  // Delete with a predicate
  queries.push_back("DELETE FROM foo WHERE id=3;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }

    EXPECT_TRUE(stmt_list->GetNumStatements() == 1);
    EXPECT_TRUE(stmt_list->GetStatement(0)->GetType() == StatementType::DELETE);
    auto delstmt = (parser::DeleteStatement *)stmt_list->GetStatement(0);
    EXPECT_EQ(delstmt->GetTableName(), "foo");
    EXPECT_TRUE(delstmt->expr != nullptr);

    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

}  // End test namespace
}  // End peloton namespace
