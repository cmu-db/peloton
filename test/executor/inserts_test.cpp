
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

#include "executor/insert_executor.h"
#include "common/harness.h"
#include "common/logger.h"
#include "catalog/bootstrapper.h"
#include "catalog/catalog.h"
#include "planner/insert_plan.h"
#include "expression/abstract_expression.h"
#include "parser/statement_insert.h"
#include "parser/statement_select.h"


namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class InsertTests : public PelotonTest {};

TEST_F(InsertTests, InsertRecord) {

  catalog::Bootstrapper::bootstrap();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.BeginTransaction();
  // Insert a table first
  auto id_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "dept_id", true);
  auto name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, 32,
                      "dept_name", false);

  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({id_column, name_column}));

  catalog::Bootstrapper::global_catalog->CreateDatabase(DEFAULT_DB_NAME);
  txn_manager.CommitTransaction();

  txn_manager.BeginTransaction();
  catalog::Bootstrapper::global_catalog->CreateTable(DEFAULT_DB_NAME, "TEST_TABLE", std::move(table_schema));
  txn_manager.CommitTransaction();

  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));

  auto insert_node = new parser::InsertStatement(INSERT_TYPE_VALUES);
  std::string name = "TEST_TABLE";

  std::string col_1 = "dept_id";
  std::string col_2 = "dept_name";

  insert_node->table_name = const_cast<char*>(name.c_str());

  insert_node->columns = new std::vector<char*>;
  insert_node->columns->push_back(const_cast<char*>(col_1.c_str()));
  insert_node->columns->push_back(const_cast<char*>(col_2.c_str()));

  insert_node->values = new std::vector<expression::AbstractExpression*>;
  insert_node->values->push_back(new expression::ConstantValueExpression(
            ValueFactory::GetIntegerValue(70)));

  insert_node->values->push_back(new expression::ConstantValueExpression(
            ValueFactory::GetStringValue("Hello")));

  insert_node->select = new parser::SelectStatement();
  
  planner::InsertPlan node(insert_node);
  executor::InsertExecutor executor(&node, context.get());

  EXPECT_TRUE(executor.Init());
  EXPECT_TRUE(executor.Execute());

  txn_manager.CommitTransaction();

}

}  // End test namespace
}  // End peloton namespace
