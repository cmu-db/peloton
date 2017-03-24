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
#include "expression/tuple_value_expression.h"

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

TEST_F(PostgresParserTests, UpdateTest0) {
  std::vector<std::string> queries;

  // Select with complicated where, tests both BoolExpr and AExpr
  queries.push_back(
      "UPDATE ORDER_LINE SET OL_DELIVERY_D = '2016-11-15 15:07:37' WHERE OL_O_ID = 2101 AND OL_D_ID = 2");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  auto stmt_list = parser.BuildParseTree(queries[0]).release();
  auto stmt = stmt_list->GetStatement(0);

  // Check root type
  EXPECT_EQ(stmt->GetType(), StatementType::UPDATE);
  auto update = (parser::UpdateStatement*)stmt;

  // Check table name
  auto table_ref = update->table;
  EXPECT_EQ(std::string(table_ref->table_info_->table_name), "order_line");

  // Check where expression
  auto where = update->where;
  EXPECT_EQ(where->GetExpressionType(), ExpressionType::CONJUNCTION_AND);
  EXPECT_EQ(where->GetChildrenSize(), 2);
  EXPECT_EQ(where->GetChild(0)->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(where->GetChild(1)->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(where->GetChild(0)->GetChildrenSize(), 2);
  EXPECT_EQ(where->GetChild(1)->GetChildrenSize(), 2);
  EXPECT_EQ(where->GetChild(0)->GetChild(0)->GetExpressionType(), ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(where->GetChild(1)->GetChild(0)->GetExpressionType(), ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(((expression::TupleValueExpression*)(where->GetChild(0)->GetChild(0)))->GetColumnName(), "ol_o_id");
  EXPECT_EQ(((expression::TupleValueExpression*)(where->GetChild(1)->GetChild(0)))->GetColumnName(), "ol_d_id");
  EXPECT_EQ(where->GetChild(0)->GetChild(1)->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(where->GetChild(1)->GetChild(1)->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(((expression::ConstantValueExpression*)(where->GetChild(0)->GetChild(1)))->GetValue().ToString(), "2101");
  EXPECT_EQ(((expression::ConstantValueExpression*)(where->GetChild(1)->GetChild(1)))->GetValue().ToString(), "2");

  // Check update clause
  auto update_clause = update->updates->at(0);
  EXPECT_EQ(std::string(update_clause->column), "ol_delivery_d");
  auto value = update_clause->value;
  EXPECT_EQ(value->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(((expression::ConstantValueExpression*)value)->GetValue().ToString(), "2016-11-15 15:07:37");
  EXPECT_EQ(((expression::ConstantValueExpression*)value)->GetValueType(), type::Type::TypeId::VARCHAR);

}

}  // End test namespace
}  // End peloton namespace
