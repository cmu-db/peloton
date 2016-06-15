//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_test.cpp
//
// Identification: test/executor/create_test.cpp
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
#include "planner/create_plan.h"
#include "executor/create_executer.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CreateTests : public PelotonTest {};

TEST_F(CreateTests, CreatingTable) {

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
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::CreatePlan node("department_table", std::move(table_schema));
  executor::CreateExecutor executor(&node, context.get());
  executor.Init();
  executor.Execute();
//  txn_manager.CommitTransaction();
  EXPECT_EQ(global_catalog->GetDatabaseWithName("default_database")->GetTableCount(), 1);

}


}
}
