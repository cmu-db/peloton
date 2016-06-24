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
  std::unique_ptr<catalog::Schema> table_schema2(new catalog::Schema({id_column, name_column}));

  catalog::Bootstrapper::global_catalog->CreateDatabase("default_database");
 txn_manager.CommitTransaction();

 txn_manager.BeginTransaction();
 catalog::Bootstrapper::global_catalog->CreateTable("default_database", "department_table", std::move(table_schema));
  txn_manager.CommitTransaction();

  txn_manager.BeginTransaction();
  catalog::Bootstrapper::global_catalog->CreateTable("default_database", "department_table_2", std::move(table_schema2));
  txn_manager.CommitTransaction();

  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseWithName("default_database")->GetTableCount(), 2);

  // Now dropping the table using the executer
  txn_manager.BeginTransaction();
  catalog::Bootstrapper::global_catalog->DropTable("default_database", "department_table");
  txn_manager.CommitTransaction();
  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseWithName("default_database")->GetTableCount(), 1);

}


}
}
