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
#include "common/logger.h"
#include "common/macros.h"
#include "expression/operator_expression.h"
#include "expression/tuple_value_expression.h"
#include "parser/parser.h"
#include "parser/postgresparser.h"

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
  queries.push_back(
      "SELECT * FROM foo INNER JOIN bar ON foo.id=bar.id AND foo.val > "
      "bar.val;");
  queries.push_back("SELECT * FROM foo LEFT JOIN bar ON foo.id=bar.id;");
  queries.push_back(
      "SELECT * FROM foo RIGHT JOIN bar ON foo.id=bar.id AND foo.val > "
      "bar.val;");
  queries.push_back(
      "SELECT * FROM foo FULL OUTER JOIN bar ON foo.id=bar.id AND foo.val > "
      "bar.val;");

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
  queries.push_back(
      "SELECT foo.name FROM (SELECT * FROM bar) as b, foo, bar WHERE foo.id = "
      "b.id;");

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

TEST_F(PostgresParserTests, ColumnUpdateTest) {
  std::vector<std::string> queries;

  // Select with complicated where, tests both BoolExpr and AExpr
  queries.push_back(
      "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE, C_DELIVERY_CNT = "
      "C_DELIVERY_CNT WHERE C_W_ID = 2");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());

    EXPECT_EQ(stmt_list->statements.size(), 1);
    auto sql_stmt = stmt_list->statements[0];

    EXPECT_EQ(sql_stmt->GetType(), StatementType::UPDATE);
    auto update_stmt = (parser::UpdateStatement *)(sql_stmt);
    auto table = update_stmt->table;
    auto updates = update_stmt->updates;
    auto where_clause = update_stmt->where;

    EXPECT_NE(table, nullptr);
    EXPECT_NE(table->table_info_, nullptr);
    EXPECT_NE(table->table_info_->table_name, nullptr);
    EXPECT_EQ(std::string(table->table_info_->table_name),
              std::string("customer"));

    EXPECT_NE(updates, nullptr);
    EXPECT_EQ(updates->size(), 2);
    EXPECT_EQ(std::string((*updates)[0]->column), std::string("c_balance"));
    EXPECT_EQ((*updates)[0]->value->GetExpressionType(),
              ExpressionType::VALUE_TUPLE);
    expression::TupleValueExpression *column_value_0 =
        (expression::TupleValueExpression *)((*updates)[0]->value);
    EXPECT_EQ(column_value_0->GetColumnName(), std::string("c_balance"));

    EXPECT_EQ(std::string((*updates)[1]->column),
              std::string("c_delivery_cnt"));
    EXPECT_EQ((*updates)[1]->value->GetExpressionType(),
              ExpressionType::VALUE_TUPLE);
    expression::TupleValueExpression *column_value_1 =
        (expression::TupleValueExpression *)((*updates)[1]->value);
    EXPECT_EQ(column_value_1->GetColumnName(), std::string("c_delivery_cnt"));

    EXPECT_NE(where_clause, nullptr);
    EXPECT_EQ(where_clause->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
    // auto eq_expr = static_cast<express::ComparisonExpression> (where_clause);
    auto left_child = where_clause->GetChild(0);
    auto right_child = where_clause->GetChild(1);
    EXPECT_EQ(left_child->GetExpressionType(), ExpressionType::VALUE_TUPLE);
    auto left_tuple = (expression::TupleValueExpression *)(left_child);
    EXPECT_EQ(left_tuple->GetColumnName(), std::string("c_w_id"));

    EXPECT_EQ(right_child->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
    auto right_const = (expression::ConstantValueExpression *)(right_child);

    EXPECT_EQ(right_const->GetValue().ToString(), std::string("2"));

    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, ExpressionUpdateTest) {
  std::string query =
      "UPDATE STOCK SET S_QUANTITY = 48.0 , S_YTD = S_YTD + 1 "
      "WHERE S_I_ID = 68999 AND S_W_ID = 4";
  auto &parser = parser::PostgresParser::GetInstance();
  parser::SQLStatementList *stmt_list = parser.BuildParseTree(query).release();
  EXPECT_TRUE(stmt_list->is_valid);

  auto update_stmt = (parser::UpdateStatement *)stmt_list->GetStatements()[0];
  EXPECT_EQ(std::string(update_stmt->table->table_info_->table_name), "stock");

  // TODO: Uncomment when the AExpressionTransfrom supports operator expression
  // Test First Set Condition
  EXPECT_EQ(std::string(update_stmt->updates->at(0)->column), "s_quantity");
  auto constant =
      (expression::ConstantValueExpression *)update_stmt->updates->at(0)->value;
  EXPECT_TRUE(constant->GetValue().CompareEquals(
      type::ValueFactory::GetDecimalValue(48)));

  // Test Second Set Condition
  EXPECT_EQ(std::string(update_stmt->updates->at(1)->column), "s_ytd");
  auto op_expr =
      (expression::OperatorExpression *)update_stmt->updates->at(1)->value;
  auto child1 = (expression::TupleValueExpression *)op_expr->GetChild(0);
  EXPECT_EQ(child1->GetColumnName(), "s_ytd");
  auto child2 = (expression::ConstantValueExpression *)op_expr->GetChild(1);
  EXPECT_TRUE(
      child2->GetValue().CompareEquals(type::ValueFactory::GetIntegerValue(1)));

  // Test Where clause
  auto where = (expression::OperatorExpression *)update_stmt->where;
  EXPECT_EQ(where->GetExpressionType(), ExpressionType::CONJUNCTION_AND);
  auto cond1 = (expression::OperatorExpression *)where->GetChild(0);
  EXPECT_EQ(cond1->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  auto column = (expression::TupleValueExpression *)cond1->GetChild(0);
  EXPECT_EQ(column->GetColumnName(), "s_i_id");
  constant = (expression::ConstantValueExpression *)cond1->GetChild(1);
  EXPECT_TRUE(constant->GetValue().CompareEquals(
      type::ValueFactory::GetIntegerValue(68999)));
  auto cond2 = (expression::OperatorExpression *)where->GetChild(1);
  EXPECT_EQ(cond2->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  column = (expression::TupleValueExpression *)cond2->GetChild(0);
  EXPECT_EQ(column->GetColumnName(), "s_w_id");
  constant = (expression::ConstantValueExpression *)cond2->GetChild(1);
  EXPECT_TRUE(constant->GetValue().CompareEquals(
      type::ValueFactory::GetIntegerValue(4)));

  delete stmt_list;
}

TEST_F(PostgresParserTests, StringUpdateTest) {
  std::vector<std::string> queries;

  // Select with complicated where, tests both BoolExpr and AExpr
  queries.push_back(
      "UPDATE ORDER_LINE SET OL_DELIVERY_D = '2016-11-15 15:07:37' WHERE "
      "OL_O_ID = 2101 AND OL_D_ID = 2");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  auto stmt_list = parser.BuildParseTree(queries[0]).release();
  auto stmt = stmt_list->GetStatement(0);

  // Check root type
  EXPECT_EQ(stmt->GetType(), StatementType::UPDATE);
  auto update = (parser::UpdateStatement *)stmt;

  // Check table name
  auto table_ref = update->table;
  EXPECT_EQ(std::string(table_ref->table_info_->table_name), "order_line");

  // Check where expression
  auto where = update->where;
  EXPECT_EQ(where->GetExpressionType(), ExpressionType::CONJUNCTION_AND);
  EXPECT_EQ(where->GetChildrenSize(), 2);
  EXPECT_EQ(where->GetChild(0)->GetExpressionType(),
            ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(where->GetChild(1)->GetExpressionType(),
            ExpressionType::COMPARE_EQUAL);
  EXPECT_EQ(where->GetChild(0)->GetChildrenSize(), 2);
  EXPECT_EQ(where->GetChild(1)->GetChildrenSize(), 2);
  EXPECT_EQ(where->GetChild(0)->GetChild(0)->GetExpressionType(),
            ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(where->GetChild(1)->GetChild(0)->GetExpressionType(),
            ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(
      ((expression::TupleValueExpression *)(where->GetChild(0)->GetChild(0)))
          ->GetColumnName(),
      "ol_o_id");
  EXPECT_EQ(
      ((expression::TupleValueExpression *)(where->GetChild(1)->GetChild(0)))
          ->GetColumnName(),
      "ol_d_id");
  EXPECT_EQ(where->GetChild(0)->GetChild(1)->GetExpressionType(),
            ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(where->GetChild(1)->GetChild(1)->GetExpressionType(),
            ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(
      ((expression::ConstantValueExpression *)(where->GetChild(0)->GetChild(1)))
          ->GetValue()
          .ToString(),
      "2101");
  EXPECT_EQ(
      ((expression::ConstantValueExpression *)(where->GetChild(1)->GetChild(1)))
          ->GetValue()
          .ToString(),
      "2");

  // Check update clause
  auto update_clause = update->updates->at(0);
  EXPECT_EQ(std::string(update_clause->column), "ol_delivery_d");
  auto value = update_clause->value;
  EXPECT_EQ(value->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(
      ((expression::ConstantValueExpression *)value)->GetValue().ToString(),
      "2016-11-15 15:07:37");
  EXPECT_EQ(((expression::ConstantValueExpression *)value)->GetValueType(),
            type::Type::TypeId::VARCHAR);

  delete stmt_list;
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

    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, InsertTest) {
  std::vector<std::string> queries;

  // Insert multiple tuples into the table
  queries.push_back("INSERT INTO foo VALUES (1, 2, 3), (4, 5, 6);");

  auto parser = parser::PostgresParser::GetInstance();
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    auto stmt_list = parser.BuildParseTree(query).release();
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }

    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());

    EXPECT_EQ(1, stmt_list->GetNumStatements());
    EXPECT_TRUE(stmt_list->GetStatement(0)->GetType() == StatementType::INSERT);
    auto insert_stmt = (parser::InsertStatement *)stmt_list->GetStatement(0);
    EXPECT_EQ("foo", insert_stmt->GetTableName());
    EXPECT_TRUE(insert_stmt->insert_values != nullptr);
    EXPECT_EQ(2, insert_stmt->insert_values->size());

    type::Value five = type::ValueFactory::GetIntegerValue(5);
    type::CmpBool res = five.CompareEquals(
        ((expression::ConstantValueExpression *)insert_stmt->insert_values
             ->at(1)
             ->at(1))
            ->GetValue());
    EXPECT_EQ(1, res);

    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, CreateTest) {
  std::vector<std::string> queries;

  // Select with complicated where, tests both BoolExpr and AExpr
  queries.push_back("CREATE TABLE Persons ("
                     "PersonID int, LastName varchar(255));");
//  queries.push_back("CREATE INDEX idx_pname ON Persons (LastName, FirstName);");
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
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

TEST_F(PostgresParserTests, InsertIntoSelectTest) {
  std::vector<std::string> queries;

  // insert into a table with select sub-query
  queries.push_back("INSERT INTO foo select * from bar where id = 5;");

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

    EXPECT_EQ(1, stmt_list->GetNumStatements());
    EXPECT_TRUE(stmt_list->GetStatement(0)->GetType() == StatementType::INSERT);
    auto insert_stmt = (parser::InsertStatement *)stmt_list->GetStatement(0);
    EXPECT_EQ("foo", insert_stmt->GetTableName());
    EXPECT_TRUE(insert_stmt->insert_values == nullptr);
    EXPECT_TRUE(insert_stmt->select->GetType() == StatementType::SELECT);
    EXPECT_EQ("bar",
              std::string(insert_stmt->select->from_table->GetTableName()));
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    delete stmt_list;
  }
}

}  // End test namespace
}  // End peloton namespace
