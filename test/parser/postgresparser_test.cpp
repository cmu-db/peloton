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
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "expression/function_expression.h"
#include "expression/operator_expression.h"
#include "expression/tuple_value_expression.h"
#include "parser/pg_trigger.h"
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
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
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
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
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
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());

    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
  }
}

TEST_F(PostgresParserTests, OrderByTest) {
  auto parser = parser::PostgresParser::GetInstance();
  // SELECT * FROM foo ORDER BY id;
  std::string query = "SELECT * FROM foo ORDER BY id;";
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  auto sql_stmt = stmt_list->statements[0].get();
  EXPECT_EQ(sql_stmt->GetType(), StatementType::SELECT);
  auto select_stmt = (parser::SelectStatement *)(sql_stmt);
  auto order_by = select_stmt->order.get();
  EXPECT_NE(order_by, nullptr);

  EXPECT_EQ(order_by->types.size(), 1);
  EXPECT_EQ(order_by->exprs.size(), 1);
  EXPECT_EQ(order_by->types.at(0), parser::OrderType::kOrderAsc);
  auto expr = order_by->exprs.at(0).get();
  EXPECT_EQ(expr->GetExpressionType(), ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(((expression::TupleValueExpression *)expr)->GetColumnName(), "id");

  // SELECT * FROM foo ORDER BY id ASC;
  query = "SELECT * FROM foo ORDER BY id ASC;";
  stmt_list.reset(parser.BuildParseTree(query).release());
  sql_stmt = stmt_list->statements[0].get();
  EXPECT_EQ(sql_stmt->GetType(), StatementType::SELECT);
  select_stmt = (parser::SelectStatement *)(sql_stmt);
  order_by = select_stmt->order.get();
  EXPECT_NE(order_by, nullptr);

  EXPECT_EQ(order_by->types.size(), 1);
  EXPECT_EQ(order_by->exprs.size(), 1);
  EXPECT_EQ(order_by->types.at(0), parser::OrderType::kOrderAsc);
  expr = order_by->exprs.at(0).get();
  EXPECT_EQ(expr->GetExpressionType(), ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(((expression::TupleValueExpression *)expr)->GetColumnName(), "id");

  // SELECT * FROM foo ORDER BY id DESC;
  query = "SELECT * FROM foo ORDER BY id DESC;";
  stmt_list.reset(parser.BuildParseTree(query).release());
  sql_stmt = stmt_list->statements[0].get();
  EXPECT_EQ(sql_stmt->GetType(), StatementType::SELECT);
  select_stmt = (parser::SelectStatement *)(sql_stmt);
  order_by = select_stmt->order.get();
  EXPECT_NE(order_by, nullptr);

  EXPECT_EQ(order_by->types.size(), 1);
  EXPECT_EQ(order_by->exprs.size(), 1);
  EXPECT_EQ(order_by->types.at(0), parser::OrderType::kOrderDesc);
  expr = order_by->exprs.at(0).get();
  EXPECT_EQ(expr->GetExpressionType(), ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(((expression::TupleValueExpression *)expr)->GetColumnName(), "id");

  // SELECT * FROM foo ORDER BY id, name;
  query = "SELECT * FROM foo ORDER BY id, name;";
  stmt_list.reset(parser.BuildParseTree(query).release());
  sql_stmt = stmt_list->statements[0].get();
  EXPECT_EQ(sql_stmt->GetType(), StatementType::SELECT);
  select_stmt = (parser::SelectStatement *)(sql_stmt);
  order_by = select_stmt->order.get();
  EXPECT_NE(order_by, nullptr);

  EXPECT_EQ(order_by->types.size(), 2);
  EXPECT_EQ(order_by->exprs.size(), 2);
  EXPECT_EQ(order_by->types.at(0), parser::OrderType::kOrderAsc);
  expr = order_by->exprs.at(0).get();
  EXPECT_EQ(expr->GetExpressionType(), ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(((expression::TupleValueExpression *)expr)->GetColumnName(), "id");
  expr = order_by->exprs.at(1).get();
  EXPECT_EQ(expr->GetExpressionType(), ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(((expression::TupleValueExpression *)expr)->GetColumnName(),
            "name");

  // SELECT * FROM foo ORDER BY id, name;
  query = "SELECT * FROM foo ORDER BY id, name DESC;";
  stmt_list.reset(parser.BuildParseTree(query).release());
  sql_stmt = stmt_list->statements[0].get();
  EXPECT_EQ(sql_stmt->GetType(), StatementType::SELECT);
  select_stmt = (parser::SelectStatement *)(sql_stmt);
  order_by = select_stmt->order.get();
  EXPECT_NE(order_by, nullptr);

  EXPECT_EQ(order_by->types.size(), 2);
  EXPECT_EQ(order_by->exprs.size(), 2);
  EXPECT_EQ(order_by->types.at(0), parser::OrderType::kOrderAsc);
  expr = order_by->exprs.at(0).get();
  EXPECT_EQ(expr->GetExpressionType(), ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(((expression::TupleValueExpression *)expr)->GetColumnName(), "id");
  EXPECT_EQ(order_by->types.at(1), parser::OrderType::kOrderDesc);
  expr = order_by->exprs.at(1).get();
  EXPECT_EQ(expr->GetExpressionType(), ExpressionType::VALUE_TUPLE);
  EXPECT_EQ(((expression::TupleValueExpression *)expr)->GetColumnName(),
            "name");
}

TEST_F(PostgresParserTests, ConstTest) {
  std::vector<std::string> queries;

  // Select constants
  queries.push_back("SELECT 'str', 1 FROM foo;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
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

  queries.push_back(
      "SELECT * FROM foo JOIN bar ON foo.id=bar.id JOIN baz ON "
      "foo.id2=baz.id2;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    // Test for multiple table join
    if (ii == 5) {
      auto select_stmt = reinterpret_cast<parser::SelectStatement *>(
          stmt_list->statements[0].get());
      auto join_table = select_stmt->from_table.get();
      EXPECT_TRUE(join_table->type == TableReferenceType::JOIN);
      auto l_join = join_table->join->left.get();
      auto r_table = join_table->join->right.get();
      EXPECT_TRUE(l_join->type == TableReferenceType::JOIN);
      EXPECT_TRUE(r_table->type == TableReferenceType::NAME);
      LOG_INFO("condition 0 : %s",
               join_table->join->condition->GetInfo().c_str());
      LOG_INFO("condition 0 : %s", l_join->join->condition->GetInfo().c_str());
    }
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
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
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
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }
    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
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
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
    EXPECT_TRUE(stmt_list->is_valid);
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());

    EXPECT_EQ(stmt_list->statements.size(), 1);
    auto sql_stmt = stmt_list->statements[0].get();

    EXPECT_EQ(sql_stmt->GetType(), StatementType::UPDATE);
    auto update_stmt = (parser::UpdateStatement *)(sql_stmt);
    auto table = update_stmt->table.get();
    auto &updates = update_stmt->updates;
    auto where_clause = update_stmt->where.get();

    EXPECT_NE(table, nullptr);
    EXPECT_NE(table->table_info_, nullptr);
    EXPECT_EQ(table->table_info_->table_name, "customer");

    EXPECT_EQ(updates.size(), 2);
    EXPECT_EQ(updates[0]->column, "c_balance");
    EXPECT_EQ(updates[0]->value->GetExpressionType(),
              ExpressionType::VALUE_TUPLE);
    expression::TupleValueExpression *column_value_0 =
        (expression::TupleValueExpression *)(updates[0]->value.get());
    EXPECT_EQ(column_value_0->GetColumnName(), "c_balance");

    EXPECT_EQ(updates[1]->column, "c_delivery_cnt");
    EXPECT_EQ(updates[1]->value->GetExpressionType(),
              ExpressionType::VALUE_TUPLE);
    expression::TupleValueExpression *column_value_1 =
        (expression::TupleValueExpression *)(updates[1]->value.get());
    EXPECT_EQ(column_value_1->GetColumnName(), "c_delivery_cnt");

    EXPECT_NE(where_clause, nullptr);
    EXPECT_EQ(where_clause->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
    // auto eq_expr = static_cast<express::ComparisonExpression> (where_clause);
    auto left_child = where_clause->GetChild(0);
    auto right_child = where_clause->GetChild(1);
    EXPECT_EQ(left_child->GetExpressionType(), ExpressionType::VALUE_TUPLE);
    auto left_tuple = (expression::TupleValueExpression *)(left_child);
    EXPECT_EQ(left_tuple->GetColumnName(), "c_w_id");

    EXPECT_EQ(right_child->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
    auto right_const = (expression::ConstantValueExpression *)(right_child);

    EXPECT_EQ(right_const->GetValue().ToString(), "2");
  }
}

TEST_F(PostgresParserTests, ExpressionUpdateTest) {
  std::string query =
      "UPDATE STOCK SET S_QUANTITY = 48.0 , S_YTD = S_YTD + 1 "
      "WHERE S_I_ID = 68999 AND S_W_ID = 4";
  auto &parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);

  auto update_stmt =
      (parser::UpdateStatement *)stmt_list->GetStatements()[0].get();
  EXPECT_EQ(update_stmt->table->table_info_->table_name, "stock");

  // TODO: Uncomment when the AExpressionTransfrom supports operator expression
  // Test First Set Condition
  EXPECT_EQ(update_stmt->updates.at(0)->column, "s_quantity");
  auto constant =
      (expression::ConstantValueExpression *)update_stmt->updates.at(0)
          ->value.get();
  EXPECT_EQ(CmpBool::TRUE, constant->GetValue().CompareEquals(
                               type::ValueFactory::GetDecimalValue(48)));

  // Test Second Set Condition
  EXPECT_EQ(update_stmt->updates.at(1)->column, "s_ytd");
  auto op_expr =
      (expression::OperatorExpression *)update_stmt->updates.at(1)->value.get();
  auto child1 = (expression::TupleValueExpression *)op_expr->GetChild(0);
  EXPECT_EQ(child1->GetColumnName(), "s_ytd");
  auto child2 = (expression::ConstantValueExpression *)op_expr->GetChild(1);
  EXPECT_EQ(CmpBool::TRUE, child2->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(1)));

  // Test Where clause
  auto where = (expression::OperatorExpression *)update_stmt->where.get();
  EXPECT_EQ(where->GetExpressionType(), ExpressionType::CONJUNCTION_AND);
  auto cond1 = (expression::OperatorExpression *)where->GetChild(0);
  EXPECT_EQ(cond1->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  auto column = (expression::TupleValueExpression *)cond1->GetChild(0);
  EXPECT_EQ(column->GetColumnName(), "s_i_id");
  constant = (expression::ConstantValueExpression *)cond1->GetChild(1);
  EXPECT_EQ(CmpBool::TRUE, constant->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(68999)));
  auto cond2 = (expression::OperatorExpression *)where->GetChild(1);
  EXPECT_EQ(cond2->GetExpressionType(), ExpressionType::COMPARE_EQUAL);
  column = (expression::TupleValueExpression *)cond2->GetChild(0);
  EXPECT_EQ(column->GetColumnName(), "s_w_id");
  constant = (expression::ConstantValueExpression *)cond2->GetChild(1);
  EXPECT_EQ(CmpBool::TRUE, constant->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(4)));
}

TEST_F(PostgresParserTests, StringUpdateTest) {
  std::vector<std::string> queries;

  // Select with complicated where, tests both BoolExpr and AExpr
  queries.push_back(
      "UPDATE ORDER_LINE SET OL_DELIVERY_D = '2016-11-15 15:07:37' WHERE "
      "OL_O_ID = 2101 AND OL_D_ID = 2");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(queries[0]).release());
  auto stmt = stmt_list->GetStatement(0);

  // Check root type
  EXPECT_EQ(stmt->GetType(), StatementType::UPDATE);
  auto update = (parser::UpdateStatement *)stmt;

  // Check table name
  auto table_ref = update->table.get();
  EXPECT_EQ(table_ref->table_info_->table_name, "order_line");

  // Check where expression
  auto where = update->where.get();
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
  auto update_clause = update->updates.at(0).get();
  EXPECT_EQ(update_clause->column, "ol_delivery_d");
  auto value = update_clause->value.get();
  EXPECT_EQ(value->GetExpressionType(), ExpressionType::VALUE_CONSTANT);
  EXPECT_EQ(
      ((expression::ConstantValueExpression *)value)->GetValue().ToString(),
      "2016-11-15 15:07:37");
  EXPECT_EQ(((expression::ConstantValueExpression *)value)->GetValueType(),
            type::TypeId::VARCHAR);
}

TEST_F(PostgresParserTests, DeleteTest) {
  std::vector<std::string> queries;

  // Simple delete
  queries.push_back("DELETE FROM foo;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
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
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
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
  }
}

TEST_F(PostgresParserTests, InsertTest) {
  std::vector<std::string> queries;

  // Insert multiple tuples into the table
  queries.push_back("INSERT INTO foo VALUES (NULL, 2, 3), (4, 5, 6);");

  auto parser = parser::PostgresParser::GetInstance();
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
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
    EXPECT_EQ(2, insert_stmt->insert_values.size());

    // Test NULL Value parsing
    EXPECT_TRUE(
        ((expression::ConstantValueExpression *)insert_stmt->insert_values.at(0)
             .at(0)
             .get())
            ->GetValue()
            .IsNull());
    // Test normal value
    type::Value five = type::ValueFactory::GetIntegerValue(5);
    CmpBool res = five.CompareEquals(
        ((expression::ConstantValueExpression *)insert_stmt->insert_values.at(1)
             .at(1)
             .get())
            ->GetValue());
    EXPECT_EQ(CmpBool::TRUE, res);

    // LOG_TRACE("%d : %s", ++ii, stmt_list->GetInfo().c_str());
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
  }
}

TEST_F(PostgresParserTests, CreateTest) {
  std::string query =
      "CREATE TABLE Persons ("
      "id INT NOT NULL UNIQUE, "
      "age INT PRIMARY KEY, "
      "name VARCHAR(255), "
      "c_id INT,"
      "PRIMARY KEY (id),"
      "FOREIGN KEY (c_id) REFERENCES country (cid));";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  auto create_stmt = (parser::CreateStatement *)stmt_list->GetStatement(0);
  LOG_INFO("Statement List Info:\n%s", stmt_list->GetInfo().c_str());
  // Check column definition
  EXPECT_EQ(create_stmt->columns.size(), 5);
  // Check First column
  auto column = create_stmt->columns.at(0).get();
  EXPECT_TRUE(column->not_null);
  EXPECT_TRUE(column->unique);
  EXPECT_TRUE(column->primary);
  EXPECT_EQ(column->name, "id");
  EXPECT_EQ(parser::ColumnDefinition::DataType::INT, column->type);
  // Check Second column
  column = create_stmt->columns.at(1).get();
  EXPECT_FALSE(column->not_null);
  EXPECT_TRUE(column->primary);
  // Check Third column
  column = create_stmt->columns.at(2).get();
  EXPECT_FALSE(column->primary);
  EXPECT_EQ(column->varlen, 255);

  // Check Foreign Key Constraint
  column = create_stmt->columns.at(4).get();
  EXPECT_EQ(parser::ColumnDefinition::DataType::FOREIGN, column->type);
  EXPECT_EQ("c_id", column->foreign_key_source.at(0));
  EXPECT_EQ("cid", column->foreign_key_sink.at(0));
  EXPECT_EQ("country", column->table_info_->table_name);
}

TEST_F(PostgresParserTests, TransactionTest) {
  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree("BEGIN TRANSACTION;").release());
  auto transac_stmt =
      (parser::TransactionStatement *)stmt_list->GetStatement(0);
  EXPECT_TRUE(stmt_list->is_valid);
  EXPECT_EQ(parser::TransactionStatement::kBegin, transac_stmt->type);

  stmt_list.reset(parser.BuildParseTree("BEGIN;").release());
  transac_stmt = (parser::TransactionStatement *)stmt_list->GetStatement(0);
  EXPECT_TRUE(stmt_list->is_valid);
  EXPECT_EQ(parser::TransactionStatement::kBegin, transac_stmt->type);

  stmt_list.reset(parser.BuildParseTree("COMMIT TRANSACTION;").release());
  transac_stmt = (parser::TransactionStatement *)stmt_list->GetStatement(0);
  EXPECT_TRUE(stmt_list->is_valid);
  EXPECT_EQ(parser::TransactionStatement::kCommit, transac_stmt->type);

  stmt_list.reset(parser.BuildParseTree("ROLLBACK;").release());
  transac_stmt = (parser::TransactionStatement *)stmt_list->GetStatement(0);
  EXPECT_TRUE(stmt_list->is_valid);
  EXPECT_EQ(parser::TransactionStatement::kRollback, transac_stmt->type);
}

TEST_F(PostgresParserTests, CreateIndexTest) {
  std::string query =
      "CREATE UNIQUE INDEX IDX_ORDER ON "
      "oorder (O_W_ID, O_D_ID);";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  auto create_stmt = (parser::CreateStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  // Check attributes
  EXPECT_EQ(parser::CreateStatement::kIndex, create_stmt->type);
  EXPECT_EQ("idx_order", create_stmt->index_name);
  EXPECT_EQ("oorder", create_stmt->table_info_->table_name);
  EXPECT_TRUE(create_stmt->unique);
  EXPECT_EQ("o_w_id", create_stmt->index_attrs.at(0));
  EXPECT_EQ("o_d_id", create_stmt->index_attrs.at(1));

  query = "CREATE INDEX ii ON t USING SKIPLIST (col);";
  stmt_list.reset(parser.BuildParseTree(query).release());

  EXPECT_TRUE(stmt_list->is_valid);
  create_stmt = (parser::CreateStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  // Check attributes
  EXPECT_EQ(parser::CreateStatement::kIndex, create_stmt->type);
  EXPECT_EQ(IndexType::SKIPLIST, create_stmt->index_type);
  EXPECT_EQ("ii", create_stmt->index_name);
  EXPECT_EQ("t", create_stmt->table_info_->table_name);

  query = "CREATE INDEX ii ON t (col);";
  stmt_list.reset(parser.BuildParseTree(query).release());

  EXPECT_TRUE(stmt_list->is_valid);
  create_stmt = (parser::CreateStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  // Check attributes
  EXPECT_EQ(parser::CreateStatement::kIndex, create_stmt->type);
  EXPECT_EQ(IndexType::BWTREE, create_stmt->index_type);
  EXPECT_EQ("ii", create_stmt->index_name);
  EXPECT_EQ("t", create_stmt->table_info_->table_name);

  query = "CREATE INDEX ii ON t USING GIN (col);";

  EXPECT_THROW(parser.BuildParseTree(query), peloton::Exception);
}

TEST_F(PostgresParserTests, InsertIntoSelectTest) {
  std::vector<std::string> queries;

  // insert into a table with select sub-query
  queries.push_back("INSERT INTO foo select * from bar where id = 5;");

  auto parser = parser::PostgresParser::GetInstance();
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(query).release());
    EXPECT_TRUE(stmt_list->is_valid);
    if (stmt_list->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
                stmt_list->error_line, stmt_list->error_col);
    }

    EXPECT_EQ(1, stmt_list->GetNumStatements());
    EXPECT_TRUE(stmt_list->GetStatement(0)->GetType() == StatementType::INSERT);
    auto insert_stmt = (parser::InsertStatement *)stmt_list->GetStatement(0);
    EXPECT_EQ("foo", insert_stmt->GetTableName());
    EXPECT_TRUE(insert_stmt->insert_values.empty());
    EXPECT_TRUE(insert_stmt->select->GetType() == StatementType::SELECT);
    EXPECT_EQ("bar",
              std::string(insert_stmt->select->from_table->GetTableName()));
    LOG_INFO("%d : %s", ++ii, stmt_list->GetInfo().c_str());
  }
}

TEST_F(PostgresParserTests, CreateDbTest) {
  std::string query = "CREATE DATABASE tt";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  auto create_stmt = (parser::CreateStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  // TODO: Check attributes
  EXPECT_EQ("tt", create_stmt->GetDatabaseName());
}

TEST_F(PostgresParserTests, CreateSchemaTest) {
  std::string query = "CREATE SCHEMA tt";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  auto create_stmt = (parser::CreateStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  // Check attributes
  EXPECT_EQ("tt", create_stmt->schema_name);

  // Test default schema name
  query = "CREATE SCHEMA AUTHORIZATION joe";

  stmt_list.reset(parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  create_stmt = (parser::CreateStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  // Check attributes
  EXPECT_EQ("joe", create_stmt->schema_name);
}

TEST_F(PostgresParserTests, CreateViewTest) {
  std::string query =
      "CREATE VIEW comedies AS SELECT * FROM films "
      "WHERE kind = 'Comedy';";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  UNUSED_ATTRIBUTE auto create_stmt =
      (parser::CreateStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  // Check attributes
  EXPECT_EQ("comedies", create_stmt->view_name);
  EXPECT_TRUE(create_stmt->view_query.get() != nullptr);
  auto view_query = create_stmt->view_query.get();
  EXPECT_EQ("films", view_query->from_table.get()->GetTableName());
  EXPECT_EQ(1, view_query->select_list.size());
  EXPECT_TRUE(view_query->where_clause.get() != nullptr);
  EXPECT_EQ(ExpressionType::COMPARE_EQUAL,
            view_query->where_clause.get()->GetExpressionType());
  EXPECT_EQ(2, view_query->where_clause.get()->GetChildrenSize());
  auto left_child = view_query->where_clause.get()->GetChild(0);
  EXPECT_EQ(ExpressionType::VALUE_TUPLE, left_child->GetExpressionType());
  EXPECT_EQ("kind",
            ((expression::TupleValueExpression *)left_child)->GetColumnName());
  auto right_child = view_query->where_clause.get()->GetChild(1);
  EXPECT_EQ(ExpressionType::VALUE_CONSTANT, right_child->GetExpressionType());
}

TEST_F(PostgresParserTests, DistinctFromTest) {
  std::string query =
      "SELECT id, value FROM foo WHERE id IS DISTINCT FROM value;";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
}

TEST_F(PostgresParserTests, ConstraintTest) {
  std::string query =
      "CREATE TABLE table1 ("
      "a int DEFAULT 1+2,"
      "b int DEFAULT 1 REFERENCES table2 (bb) ON UPDATE CASCADE,"
      "c varchar(32) REFERENCES table3 (cc) MATCH FULL ON DELETE SET NULL,"
      "d int CHECK (d+1 > 0),"
      "FOREIGN KEY (d) REFERENCES table4 (dd) MATCH SIMPLE ON UPDATE SET "
      "DEFAULT"
      ");";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  //  auto create_stmt = (parser::CreateStatement*)stmt_list->GetStatement(0);
  //  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  auto create_stmt = (parser::CreateStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  // Check column definition
  EXPECT_EQ(create_stmt->columns.size(), 5);

  // Check First column
  auto column = create_stmt->columns.at(0).get();
  EXPECT_EQ("a", column->name);
  EXPECT_EQ(parser::ColumnDefinition::DataType::INT, column->type);
  EXPECT_TRUE(column->default_value != nullptr);
  auto default_expr =
      (expression::OperatorExpression *)column->default_value.get();
  EXPECT_TRUE(default_expr != nullptr);
  EXPECT_EQ(ExpressionType::OPERATOR_PLUS, default_expr->GetExpressionType());
  EXPECT_EQ(2, default_expr->GetChildrenSize());
  auto child1 =
      (expression::ConstantValueExpression *)default_expr->GetChild(0);
  EXPECT_TRUE(child1 != nullptr);
  auto child2 =
      (expression::ConstantValueExpression *)default_expr->GetChild(1);
  EXPECT_TRUE(child2 != nullptr);
  EXPECT_EQ(CmpBool::TRUE, child1->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(1)));
  EXPECT_EQ(CmpBool::TRUE, child2->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(2)));

  // Check Second column
  column = create_stmt->columns.at(1).get();
  EXPECT_EQ("b", column->name);
  EXPECT_EQ(parser::ColumnDefinition::DataType::INT, column->type);
  EXPECT_TRUE(column->foreign_key_sink.size() == 1);
  EXPECT_EQ("bb", column->foreign_key_sink.at(0));
  EXPECT_EQ("table2", column->table_info_->table_name);
  EXPECT_EQ(FKConstrActionType::CASCADE, column->foreign_key_update_action);
  EXPECT_EQ(FKConstrActionType::NOACTION, column->foreign_key_delete_action);
  EXPECT_EQ(FKConstrMatchType::SIMPLE, column->foreign_key_match_type);

  // Check Third column
  column = create_stmt->columns.at(2).get();
  EXPECT_EQ("c", column->name);
  EXPECT_EQ(parser::ColumnDefinition::DataType::VARCHAR, column->type);
  EXPECT_TRUE(column->foreign_key_sink.size() == 1);
  EXPECT_EQ("cc", column->foreign_key_sink.at(0));
  EXPECT_EQ("table3", column->table_info_->table_name);
  EXPECT_EQ(FKConstrActionType::NOACTION, column->foreign_key_update_action);
  EXPECT_EQ(FKConstrActionType::SETNULL, column->foreign_key_delete_action);
  EXPECT_EQ(FKConstrMatchType::FULL, column->foreign_key_match_type);

  // Check Fourth column
  column = create_stmt->columns.at(3).get();
  EXPECT_EQ("d", column->name);
  EXPECT_EQ(parser::ColumnDefinition::DataType::INT, column->type);
  EXPECT_TRUE(column->check_expression != nullptr);
  EXPECT_EQ(ExpressionType::COMPARE_GREATERTHAN,
            column->check_expression->GetExpressionType());
  EXPECT_EQ(2, column->check_expression->GetChildrenSize());
  auto check_child1 =
      (expression::OperatorExpression *)column->check_expression->GetChild(0);
  EXPECT_TRUE(check_child1 != nullptr);
  EXPECT_EQ(ExpressionType::OPERATOR_PLUS, check_child1->GetExpressionType());
  EXPECT_EQ(2, check_child1->GetChildrenSize());
  auto plus_child1 =
      (expression::TupleValueExpression *)check_child1->GetChild(0);
  EXPECT_TRUE(plus_child1 != nullptr);
  EXPECT_EQ("d", plus_child1->GetColumnName());
  auto plus_child2 =
      (expression::ConstantValueExpression *)check_child1->GetChild(1);
  EXPECT_TRUE(plus_child2 != nullptr);
  EXPECT_EQ(CmpBool::TRUE, plus_child2->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(1)));
  auto check_child2 =
      (expression::ConstantValueExpression *)column->check_expression->GetChild(
          1);
  EXPECT_TRUE(check_child2 != nullptr);
  EXPECT_EQ(CmpBool::TRUE, check_child2->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(0)));

  // Check Fifth column
  column = create_stmt->columns.at(4).get();
  EXPECT_EQ(parser::ColumnDefinition::DataType::FOREIGN, column->type);
  EXPECT_TRUE(column->foreign_key_source.size() == 1);
  EXPECT_EQ("d", column->foreign_key_source.at(0));
  EXPECT_TRUE(column->foreign_key_sink.size() == 1);
  EXPECT_EQ("dd", column->foreign_key_sink.at(0));
  EXPECT_EQ("table4", std::string(column->table_info_->table_name));
  EXPECT_EQ(FKConstrActionType::SETDEFAULT, column->foreign_key_update_action);
  EXPECT_EQ(FKConstrActionType::NOACTION, column->foreign_key_delete_action);
  EXPECT_EQ(FKConstrMatchType::SIMPLE, column->foreign_key_match_type);
}

TEST_F(PostgresParserTests, DataTypeTest) {
  std::string query =
      "CREATE TABLE table1 ("
      "a text,"
      "b varchar(1024),"
      "c varbinary(32)"
      ");";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  //  auto create_stmt = (parser::CreateStatement*)stmt_list->GetStatement(0);
  //  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  auto create_stmt = (parser::CreateStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  // Check column definition
  EXPECT_EQ(create_stmt->columns.size(), 3);

  // Check First column
  auto column = create_stmt->columns.at(0).get();
  EXPECT_EQ("a", column->name);
  EXPECT_EQ(type::TypeId::VARCHAR, column->GetValueType(column->type));
  EXPECT_EQ(peloton::type::PELOTON_TEXT_MAX_LEN, column->varlen);

  // Check Second column
  column = create_stmt->columns.at(1).get();
  EXPECT_EQ("b", column->name);
  EXPECT_EQ(type::TypeId::VARCHAR, column->GetValueType(column->type));
  EXPECT_EQ(1024, column->varlen);

  // Check Third column
  column = create_stmt->columns.at(2).get();
  EXPECT_EQ("c", column->name);
  EXPECT_EQ(type::TypeId::VARBINARY, column->GetValueType(column->type));
  EXPECT_EQ(32, column->varlen);
}

TEST_F(PostgresParserTests, CreateTriggerTest) {
  auto parser = parser::PostgresParser::GetInstance();

  std::string query =
      "CREATE TRIGGER check_update "
      "BEFORE UPDATE OF balance ON accounts "
      "FOR EACH ROW "
      "WHEN (OLD.balance <> NEW.balance) "
      "EXECUTE PROCEDURE check_account_update();";
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  if (!stmt_list->is_valid) {
    LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
              stmt_list->error_line, stmt_list->error_col);
  }
  EXPECT_EQ(StatementType::CREATE, stmt_list->GetStatement(0)->GetType());
  auto create_trigger_stmt =
      static_cast<parser::CreateStatement *>(stmt_list->GetStatement(0));

  // The following code checks the type and contents in the create statement
  // are identical to what is specified in the query.

  // create type
  EXPECT_EQ(parser::CreateStatement::CreateType::kTrigger,
            create_trigger_stmt->type);
  // trigger name
  EXPECT_EQ("check_update", create_trigger_stmt->trigger_name);
  // table name
  EXPECT_EQ("accounts", create_trigger_stmt->GetTableName());

  // funcname
  // the function invoked by this trigger
  std::vector<std::string> &funcname = create_trigger_stmt->trigger_funcname;
  EXPECT_EQ(1, funcname.size());
  EXPECT_EQ("check_account_update", funcname[0]);
  // args
  // arguments in the fuction
  EXPECT_EQ(0, create_trigger_stmt->trigger_args.size());
  // columns
  std::vector<std::string> &columns = create_trigger_stmt->trigger_columns;
  EXPECT_EQ(1, columns.size());
  EXPECT_EQ("balance", columns[0]);
  // when
  // Check the expression tree of trigger_when is identical to the query
  // Need to check type and value of each node.
  auto &when = create_trigger_stmt->trigger_when;
  EXPECT_NE(nullptr, when);
  EXPECT_EQ(ExpressionType::COMPARE_NOTEQUAL, when->GetExpressionType());
  EXPECT_EQ(2, when->GetChildrenSize());
  auto left = when->GetChild(0);
  auto right = when->GetChild(1);
  EXPECT_EQ(ExpressionType::VALUE_TUPLE, left->GetExpressionType());
  EXPECT_EQ("old", static_cast<const expression::TupleValueExpression *>(left)
                       ->GetTableName());
  EXPECT_EQ("balance",
            static_cast<const expression::TupleValueExpression *>(left)
                ->GetColumnName());
  EXPECT_EQ(ExpressionType::VALUE_TUPLE, right->GetExpressionType());
  EXPECT_EQ("new", static_cast<const expression::TupleValueExpression *>(right)
                       ->GetTableName());
  EXPECT_EQ("balance",
            static_cast<const expression::TupleValueExpression *>(right)
                ->GetColumnName());
  // level
  // the level is for each row
  EXPECT_TRUE(TRIGGER_FOR_ROW(create_trigger_stmt->trigger_type));
  // timing
  // timing is before
  EXPECT_TRUE(TRIGGER_FOR_BEFORE(create_trigger_stmt->trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_AFTER(create_trigger_stmt->trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_INSTEAD(create_trigger_stmt->trigger_type));
  // event
  // event is update
  EXPECT_TRUE(TRIGGER_FOR_UPDATE(create_trigger_stmt->trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_INSERT(create_trigger_stmt->trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_DELETE(create_trigger_stmt->trigger_type));
  EXPECT_FALSE(TRIGGER_FOR_TRUNCATE(create_trigger_stmt->trigger_type));
}

TEST_F(PostgresParserTests, DropTriggerTest) {
  auto parser = parser::PostgresParser::GetInstance();
  std::string query = "DROP TRIGGER if_dist_exists ON films;";
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  if (!stmt_list->is_valid) {
    LOG_ERROR("Message: %s, line: %d, col: %d", stmt_list->parser_msg,
              stmt_list->error_line, stmt_list->error_col);
  }
  EXPECT_EQ(StatementType::DROP, stmt_list->GetStatement(0)->GetType());
  auto drop_trigger_stmt =
      static_cast<parser::DropStatement *>(stmt_list->GetStatement(0));
  // drop type
  EXPECT_EQ(parser::DropStatement::EntityType::kTrigger,
            drop_trigger_stmt->GetDropType());
  // trigger name
  EXPECT_EQ("if_dist_exists", drop_trigger_stmt->GetTriggerName());
  // table name
  EXPECT_EQ("films", drop_trigger_stmt->GetTriggerTableName());
}

TEST_F(PostgresParserTests, FuncCallTest) {
  std::string query = "SELECT add(1,a), chr(99) FROM TEST WHERE FUN(b) > 2";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
  //  auto create_stmt = (parser::CreateStatement*)stmt_list->GetStatement(0);
  //  LOG_INFO("%s", stmt_list->GetInfo().c_str());
  auto select_stmt = (parser::SelectStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());

  // Check ADD(1,a)
  auto fun_expr =
      (expression::FunctionExpression *)(select_stmt->select_list.at(0).get());
  EXPECT_TRUE(fun_expr != nullptr);
  EXPECT_EQ("add", fun_expr->GetFuncName());
  EXPECT_EQ(2, fun_expr->GetChildrenSize());
  auto const_expr =
      (expression::ConstantValueExpression *)fun_expr->GetChild(0);
  EXPECT_TRUE(const_expr != nullptr);
  EXPECT_EQ(CmpBool::TRUE, const_expr->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(1)));
  auto tv_expr = (expression::TupleValueExpression *)fun_expr->GetChild(1);
  EXPECT_TRUE(tv_expr != nullptr);
  EXPECT_EQ("a", tv_expr->GetColumnName());

  // Check chr(2)
  fun_expr =
      (expression::FunctionExpression *)(select_stmt->select_list.at(1).get());
  EXPECT_TRUE(fun_expr != nullptr);
  EXPECT_EQ("chr", fun_expr->GetFuncName());
  EXPECT_EQ(1, fun_expr->GetChildrenSize());
  const_expr = (expression::ConstantValueExpression *)fun_expr->GetChild(0);
  EXPECT_TRUE(const_expr != nullptr);
  EXPECT_EQ(CmpBool::TRUE, const_expr->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(99)));

  // Check FUN(b) > 2
  auto op_expr =
      (expression::OperatorExpression *)select_stmt->where_clause.get();
  EXPECT_TRUE(op_expr != nullptr);
  EXPECT_EQ(ExpressionType::COMPARE_GREATERTHAN, op_expr->GetExpressionType());
  fun_expr = (expression::FunctionExpression *)op_expr->GetChild(0);
  EXPECT_EQ("fun", fun_expr->GetFuncName());
  EXPECT_EQ(1, fun_expr->GetChildrenSize());
  tv_expr = (expression::TupleValueExpression *)fun_expr->GetChild(0);
  EXPECT_TRUE(tv_expr != nullptr);
  EXPECT_EQ("b", tv_expr->GetColumnName());
  const_expr = (expression::ConstantValueExpression *)op_expr->GetChild(1);
  EXPECT_TRUE(const_expr != nullptr);
  EXPECT_EQ(CmpBool::TRUE, const_expr->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(2)));
}

TEST_F(PostgresParserTests, UDFFuncCallTest) {
  std::string query = "SELECT increment(1,b) FROM TEST;";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);

  auto select_stmt = (parser::SelectStatement *)stmt_list->GetStatement(0);
  LOG_INFO("%s", stmt_list->GetInfo().c_str());

  auto fun_expr =
      (expression::FunctionExpression *)(select_stmt->select_list.at(0).get());
  EXPECT_TRUE(fun_expr != nullptr);
  EXPECT_EQ("increment", fun_expr->GetFuncName());
  EXPECT_EQ(2, fun_expr->GetChildrenSize());
  auto const_expr =
      (expression::ConstantValueExpression *)fun_expr->GetChild(0);
  EXPECT_TRUE(const_expr != nullptr);
  EXPECT_EQ(CmpBool::TRUE, const_expr->GetValue().CompareEquals(
                               type::ValueFactory::GetIntegerValue(1)));

  auto tv_expr = (expression::TupleValueExpression *)fun_expr->GetChild(1);
  EXPECT_TRUE(tv_expr != nullptr);
  EXPECT_EQ("b", tv_expr->GetColumnName());
}

TEST_F(PostgresParserTests, CaseTest) {
  std::string query = "SELECT id, case when id=100 then 1 else 0 end from tbl;";

  auto parser = parser::PostgresParser::GetInstance();
  std::unique_ptr<parser::SQLStatementList> stmt_list(
      parser.BuildParseTree(query).release());
  EXPECT_TRUE(stmt_list->is_valid);
}

TEST_F(PostgresParserTests, DateTypeTest) {
  std::vector<std::string> valid_queries;
  valid_queries.push_back(
      "INSERT INTO test_table VALUES (1, 2, "
      "'2017-01-01'::DATE);");
  valid_queries.push_back("CREATE TABLE students (name TEXT, graduation DATE)");
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : valid_queries) {
    std::unique_ptr<parser::SQLStatementList> result(
        parser::PostgresParser::ParseSQLString(query.c_str()));

    if (result->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", result->parser_msg,
                result->error_line, result->error_col);
    }
    EXPECT_EQ(result->is_valid, true);

    LOG_TRACE("%d : %s", ++ii, result->GetInfo().c_str());
  }

  // Check invalid input handling
  std::vector<std::string> invalid_queries;
  invalid_queries.push_back(
      "INSERT INTO test_table VALUES (1, 2, "
      "'2017-00-01'::DATE);");
  invalid_queries.push_back(
      "INSERT INTO test_table VALUES (1, 2, "
      "'2017-01-011'::DATE);");
  invalid_queries.push_back(
      "INSERT INTO test_table VALUES (1, 2, "
      "'2017-00-'::DATE);");
  for (auto query : invalid_queries) {
    EXPECT_THROW(parser::PostgresParser::ParseSQLString(query.c_str()),
                 peloton::Exception);
  }
}

TEST_F(PostgresParserTests, TypeCastTest) {
  std::vector<std::string> queries;
  queries.push_back("INSERT INTO test_table VALUES (1, 2, '2017'::INTEGER);");
  queries.push_back("INSERT INTO test_table VALUES (1, 2, '2017'::FLOAT);");
  queries.push_back("INSERT INTO test_table VALUES (1, 2, '2017'::DECIMAL);");
  queries.push_back("INSERT INTO test_table VALUES (1, 2, '2017'::TEXT);");
  queries.push_back("INSERT INTO test_table VALUES (1, 2, '2017'::VARCHAR);");
  // Parsing
  UNUSED_ATTRIBUTE int ii = 0;
  for (auto query : queries) {
    std::unique_ptr<parser::SQLStatementList> result(
        parser::PostgresParser::ParseSQLString(query.c_str()));

    if (result->is_valid == false) {
      LOG_ERROR("Message: %s, line: %d, col: %d", result->parser_msg,
                result->error_line, result->error_col);
    }
    EXPECT_EQ(result->is_valid, true);

    LOG_TRACE("%d : %s", ++ii, result->GetInfo().c_str());
  }
}

}  // namespace test
}  // namespace peloton
