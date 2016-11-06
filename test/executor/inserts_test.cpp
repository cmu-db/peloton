
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_test.cpp
//
// Identification: /peloton/test/executor/insert_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "gtest/gtest.h"

#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "executor/insert_executor.h"
#include "expression/abstract_expression.h"
#include "parser/statement_insert.h"
#include "parser/statement_select.h"
#include "planner/insert_plan.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class InsertTests : public PelotonTest {};

TEST_F(InsertTests, InsertRecord) {
  catalog::Catalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // Insert a table first
  auto id_column = catalog::Column(
      common::Type::INTEGER, common::Type::GetTypeSize(common::Type::INTEGER),
      "dept_id", true);
  auto name_column =
      catalog::Column(common::Type::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateTable(DEFAULT_DB_NAME, "TEST_TABLE",
                                               std::move(table_schema), txn);
  txn_manager.CommitTransaction(txn);

  auto table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, "TEST_TABLE");

  txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  std::unique_ptr<parser::InsertStatement> insert_node(
      new parser::InsertStatement(INSERT_TYPE_VALUES));

  char *name = new char[11]();
  strcpy(name, "TEST_TABLE");
  expression::ParserExpression *table_name = new expression::ParserExpression(
      EXPRESSION_TYPE_TABLE_REF, name, nullptr);

  char *col_1 = new char[8]();
  strcpy(col_1, "dept_id");

  char *col_2 = new char[10]();
  strcpy(col_2, "dept_name");

  insert_node->table_name = table_name;

  insert_node->columns = new std::vector<char *>;
  insert_node->columns->push_back(const_cast<char *>(col_1));
  insert_node->columns->push_back(const_cast<char *>(col_2));

  insert_node->insert_values = new std::vector<std::vector<expression::AbstractExpression *>*>;
  auto values_ptr = new std::vector<expression::AbstractExpression *>;
  insert_node->insert_values->push_back(values_ptr);

  values_ptr->push_back(new expression::ConstantValueExpression(
      common::ValueFactory::GetIntegerValue(70)));

  values_ptr->push_back(new expression::ConstantValueExpression(
      common::ValueFactory::GetVarcharValue("Hello")));

  insert_node->select = new parser::SelectStatement();

  planner::InsertPlan node(insert_node.get());
  executor::InsertExecutor executor(&node, context.get());

  EXPECT_TRUE(executor.Init());
  EXPECT_TRUE(executor.Execute());
  EXPECT_EQ(1, table->GetTupleCount());

  delete values_ptr->at(0);
  values_ptr->at(0) = new expression::ConstantValueExpression(
      common::ValueFactory::GetIntegerValue(80));
  planner::InsertPlan node2(insert_node.get());
  executor::InsertExecutor executor2(&node2, context.get());

  EXPECT_TRUE(executor2.Init());
  EXPECT_TRUE(executor2.Execute());
  EXPECT_EQ(2, table->GetTupleCount());

  auto values_ptr2 = new std::vector<expression::AbstractExpression *>;
  insert_node->insert_values->push_back(values_ptr2);

  values_ptr2->push_back(new expression::ConstantValueExpression(
      common::ValueFactory::GetIntegerValue(100)));

  values_ptr2->push_back(new expression::ConstantValueExpression(
      common::ValueFactory::GetVarcharValue("Hello")));

  delete values_ptr->at(0);
  values_ptr->at(0) = new expression::ConstantValueExpression(
      common::ValueFactory::GetIntegerValue(90));
  planner::InsertPlan node3(insert_node.get());
  executor::InsertExecutor executor3(&node3, context.get());

  EXPECT_TRUE(executor3.Init());
  EXPECT_TRUE(executor3.Execute());
  EXPECT_EQ(4, table->GetTupleCount());

  txn_manager.CommitTransaction(txn);

  // free the database just created
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

}  // End test namespace
}  // End peloton namespace
