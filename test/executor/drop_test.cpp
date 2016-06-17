//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_test.cpp
//
// Identification: test/executor/drop_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "gtest/gtest.h"
#include "common/harness.h"
#include "common/logger.h"
#include "catalog/bootstrapper.h"
#include "catalog/catalog.h"
#include "planner/drop_plan.h"
#include "executor/drop_executer.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class DropTests : public PelotonTest {};

TEST_F(DropTests, DroppingTable) {

	auto &bootstrapper = catalog::Bootstrapper::GetInstance();
	auto global_catalog = bootstrapper.bootstrap();

  // Insert a table first
  auto id_column =
		  catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
						  "dept_id", true);
  auto name_column =
	  catalog::Column(VALUE_TYPE_VARCHAR, 32,
					  "dept_name", false);
  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({id_column, name_column}));
  std::unique_ptr<catalog::Schema> table_schema2(new catalog::Schema({id_column, name_column}));

  global_catalog->CreateDatabase("default_database");

  std:: cout << "Database info before adding first table: " << global_catalog->GetDatabaseWithName("default_database")->GetInfo() << std::endl;
  global_catalog->CreateTable("default_database", "department_table", std::move(table_schema));
  std::cout << "First table added!" << std::endl;

  std:: cout << "Database info after adding first table: " << global_catalog->GetDatabaseWithName("default_database")->GetInfo() << std::endl;
  global_catalog->CreateTable("default_database", "department_table_2", std::move(table_schema2));
  std::cout << "Second table added!" << std::endl;
  std:: cout << "Database info after adding second table: " << global_catalog->GetDatabaseWithName("default_database")->GetInfo() << std::endl;

  EXPECT_EQ(global_catalog->GetDatabaseWithName("default_database")->GetTableCount(), 2);

  // Now dropping the table using the executer
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::DropPlan node("department_table");
  executor::DropExecutor executor(&node, context.get());
  executor.Init();
  executor.Execute();
  txn_manager.CommitTransaction();
  planner::DropPlan node2("department_table_2");
  executor::DropExecutor executor2(&node2, context.get());
  executor2.Init();
  executor2.Execute();
  EXPECT_EQ(global_catalog->GetDatabaseWithName("default_database")->GetTableCount(), 0);

}


}
}
