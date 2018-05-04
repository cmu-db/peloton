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
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/insert_executor.h"
#include "expression/constant_value_expression.h"
#include "parser/insert_statement.h"
#include "planner/insert_plan.h"
#include "type/value_factory.h"

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
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "dept_id", true);
  auto name_column =
      catalog::Column(type::TypeId::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));

  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateTable(
      DEFAULT_DB_NAME, DEFUALT_SCHEMA_NAME, "TEST_TABLE",
      std::move(table_schema), txn);

  auto table = catalog::Catalog::GetInstance()->GetTableWithName(
      DEFAULT_DB_NAME, DEFUALT_SCHEMA_NAME, "TEST_TABLE", txn);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  std::unique_ptr<parser::InsertStatement> insert_node(
      new parser::InsertStatement(InsertType::VALUES));

  std::string name = "TEST_TABLE";
  auto table_ref = new parser::TableRef(TableReferenceType::NAME);
  parser::TableInfo *table_info = new parser::TableInfo();
  table_info->table_name = name;

  std::string col_1 = "dept_id";
  std::string col_2 = "dept_name";

  table_ref->table_info_.reset(table_info);
  insert_node->table_ref_.reset(table_ref);

  insert_node->columns.push_back(col_1);
  insert_node->columns.push_back(col_2);

  insert_node->insert_values.push_back(
      std::vector<std::unique_ptr<expression::AbstractExpression>>());
  auto &values_ptr = insert_node->insert_values[0];

  values_ptr.push_back(std::unique_ptr<expression::AbstractExpression>(
      new expression::ConstantValueExpression(
          type::ValueFactory::GetIntegerValue(70))));

  values_ptr.push_back(std::unique_ptr<expression::AbstractExpression>(
      new expression::ConstantValueExpression(
          type::ValueFactory::GetVarcharValue("Hello"))));

  insert_node->select.reset(new parser::SelectStatement());

  planner::InsertPlan node(table, &insert_node->columns,
                           &insert_node->insert_values);
  executor::InsertExecutor executor(&node, context.get());

  EXPECT_TRUE(executor.Init());
  EXPECT_TRUE(executor.Execute());
  EXPECT_EQ(1, table->GetTupleCount());

  values_ptr.at(0).reset(new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(80)));
  planner::InsertPlan node2(table, &insert_node->columns,
                            &insert_node->insert_values);
  executor::InsertExecutor executor2(&node2, context.get());

  EXPECT_TRUE(executor2.Init());
  EXPECT_TRUE(executor2.Execute());
  EXPECT_EQ(2, table->GetTupleCount());

  insert_node->insert_values.push_back(
      std::vector<std::unique_ptr<expression::AbstractExpression>>());
  auto &values_ptr2 = insert_node->insert_values[1];

  values_ptr2.push_back(std::unique_ptr<expression::AbstractExpression>(
      new expression::ConstantValueExpression(
          type::ValueFactory::GetIntegerValue(100))));

  values_ptr2.push_back(std::unique_ptr<expression::AbstractExpression>(
      new expression::ConstantValueExpression(
          type::ValueFactory::GetVarcharValue("Hello"))));

  insert_node->insert_values[0].at(0).reset(
      new expression::ConstantValueExpression(
          type::ValueFactory::GetIntegerValue(90)));
  planner::InsertPlan node3(table, &insert_node->columns,
                            &insert_node->insert_values);
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

}  // namespace test
}  // namespace peloton
