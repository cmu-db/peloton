//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
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

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CatalogTests : public PelotonTest {};


TEST_F(CatalogTests, BootstrappingCatalog) {
  catalog::Bootstrapper::bootstrap();
  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseCount(), 1);
}

TEST_F(CatalogTests, CreatingDatabase) {
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	txn_manager.BeginTransaction();
	catalog::Bootstrapper::global_catalog->CreateDatabase("EMP_DB");
	txn_manager.CommitTransaction();
  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseWithName("EMP_DB")->GetDBName(), "EMP_DB");
}

TEST_F(CatalogTests, CreatingTable) {
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	txn_manager.BeginTransaction();
  auto id_column =
	      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
	                      "dept_id", true);
  auto name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, 32,
                      "dept_name", true);
  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({id_column, name_column}));
  std::unique_ptr<catalog::Schema> table_schema_2(new catalog::Schema({id_column, name_column}));
  catalog::Bootstrapper::global_catalog->CreateTable("EMP_DB", "emp_table", std::move(table_schema));
  catalog::Bootstrapper::global_catalog->CreateTable("EMP_DB", "department_table", std::move(table_schema_2));
  txn_manager.CommitTransaction();
  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseWithName("EMP_DB")->GetTableWithName("department_table")->GetSchema()->GetColumn(1).GetName(), "dept_name");
  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseWithName("catalog_db")->GetTableWithName("table_catalog")->GetNumberOfTuples(), 2);

}

TEST_F(CatalogTests, DroppingTable) {
  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseWithName("EMP_DB")->GetTableCount(), 2);
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	txn_manager.BeginTransaction();
	catalog::Bootstrapper::global_catalog->DropTable("EMP_DB", "department_table");
  txn_manager.CommitTransaction();
  catalog::Bootstrapper::global_catalog->PrintCatalogs();
  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseWithName("EMP_DB")->GetTableCount(), 1);

  // Try to drop again
  txn_manager.BeginTransaction();
  catalog::Bootstrapper::global_catalog->DropTable("EMP_DB", "department_table");
  txn_manager.CommitTransaction();
  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseWithName("EMP_DB")->GetTableCount(), 1);

  // Drop the other table
  txn_manager.BeginTransaction();
  catalog::Bootstrapper::global_catalog->DropTable("EMP_DB", "emp_table");
  txn_manager.CommitTransaction();
  EXPECT_EQ(catalog::Bootstrapper::global_catalog->GetDatabaseWithName("EMP_DB")->GetTableCount(), 0);
}

}  // End test namespace
}  // End peloton namespace
