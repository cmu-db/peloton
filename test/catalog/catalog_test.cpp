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
#include "catalog/catalog.h"

#define CATALOG_DATABASE_NAME "catalog_db"
#define DATABASE_CATALOG_NAME "database_catalog"
#define TABLE_CATALOG_NAME "table_catalog"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CatalogTests : public PelotonTest {};

TEST_F(CatalogTests, BootstrappingCatalog) {
  auto catalog = catalog::Catalog::GetInstance();
  EXPECT_EQ(catalog->GetDatabaseCount(), 1);
  storage::Database *database =
      catalog->GetDatabaseWithName(CATALOG_DATABASE_NAME);
  EXPECT_NE(database, nullptr);

  // Check metric tables
  auto db_metric_table = database->GetTableWithName(DATABASE_METRIC_NAME);
  EXPECT_NE(db_metric_table, nullptr);
}

TEST_F(CatalogTests, CreatingDatabase) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase("EMP_DB", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetDBName(),
            "EMP_DB");
}
/*
TEST_F(CatalogTests, CreatingTable) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "id", true);
  auto name_column = catalog::Column(type::Type::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  std::unique_ptr<catalog::Schema> table_schema_2(
      new catalog::Schema({id_column, name_column}));
  std::unique_ptr<catalog::Schema> table_schema_3(
      new catalog::Schema({id_column, name_column}));

  catalog::Catalog::GetInstance()->CreateTable("EMP_DB", "emp_table",
                                               std::move(table_schema), txn);
  catalog::Catalog::GetInstance()->CreateTable("EMP_DB", "department_table",
                                               std::move(table_schema_2), txn);
  catalog::Catalog::GetInstance()->CreateTable("EMP_DB", "salary_table",
                                               std::move(table_schema_3), txn);

  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableWithName("department_table")
                ->GetSchema()
                ->GetColumn(1)
                .GetName(),
            "name");
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("catalog_db")
                ->GetTableWithName("table_catalog")
                ->GetTupleCount(),
            3);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("catalog_db")
                ->GetTableWithName("table_catalog")
                ->GetSchema()
                ->GetLength(),
            72);
}

TEST_F(CatalogTests, DroppingTable) {
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableCount(),
            3);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropTable("EMP_DB", "department_table", txn);
  txn_manager.CommitTransaction(txn);
  catalog::Catalog::GetInstance()->PrintCatalogs();
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableCount(),
            2);

  // Try to drop again
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropTable("EMP_DB", "department_table", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableCount(),
            2);

  // Drop a table that does not exist
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropTable("EMP_DB", "void_table", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableCount(),
            2);

  // Drop the other table
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropTable("EMP_DB", "emp_table", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableCount(),
            1);
}

TEST_F(CatalogTests, DroppingDatabase) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName("EMP_DB", txn);

  EXPECT_THROW(catalog::Catalog::GetInstance()->GetDatabaseWithName("EMP_DB"),
               CatalogException);
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, DroppingCatalog) {
  auto catalog = catalog::Catalog::GetInstance();
  EXPECT_NE(catalog, nullptr);
}
*/
}  // End test namespace
}  // End peloton namespace
