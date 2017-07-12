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

#include "catalog/catalog.h"
#include "catalog/database_metrics_catalog.h"
#include "catalog/query_metrics_catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/storage_manager.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CatalogTests : public PelotonTest {};

TEST_F(CatalogTests, BootstrappingCatalog) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  EXPECT_EQ(storage::StorageManager::GetInstance()->GetDatabaseCount(), 1);
  storage::Database *database =
      catalog->GetDatabaseWithName(CATALOG_DATABASE_NAME);
  EXPECT_NE(database, nullptr);
  // Check database metric table
  auto db_metric_table =
      database->GetTableWithName(DATABASE_METRICS_CATALOG_NAME);
  EXPECT_NE(db_metric_table, nullptr);
}
//
TEST_F(CatalogTests, CreatingDatabase) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase("EMP_DB", txn);
  std::vector<oid_t> key_attrs =
      catalog::IndexCatalog::GetInstance()->GetIndexedAttributes(
          INDEX_CATALOG_PKEY_OID, txn);
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetDBName(),
            "EMP_DB");
  EXPECT_EQ(key_attrs.size(), 1);
  EXPECT_EQ(key_attrs[0], 0);
  // EXPECT_EQ(index_name, "pg_index_pkey");
}

TEST_F(CatalogTests, CreatingTable) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto id_column =
      catalog::Column(type::TypeId::INTEGER,
                      type::Type::GetTypeSize(type::TypeId::INTEGER), "id", true);
  id_column.AddConstraint(
      catalog::Constraint(ConstraintType::PRIMARY, "primary_key"));
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

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
  // insert random tuple into DATABASE_METRICS_CATALOG and check
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());
  catalog::DatabaseMetricsCatalog::GetInstance()->InsertDatabaseMetrics(
      2, 3, 4, 5, pool.get(), txn);
  oid_t time_stamp =
      catalog::DatabaseMetricsCatalog::GetInstance()->GetTimeStamp(2, txn);

  // inset meaningless tuple into QUERY_METRICS_CATALOG and check
  stats::QueryMetric::QueryParamBuf param;
  param.len = 1;
  param.buf = (unsigned char *)pool->Allocate(1);
  *param.buf = 'a';
  catalog::QueryMetricsCatalog::GetInstance()->InsertQueryMetrics(
      "a query", 1, 1, param, param, param, 1, 1, 1, 1, 1, 1, 1, pool.get(),
      txn);
  auto param1 = catalog::QueryMetricsCatalog::GetInstance()->GetParamTypes(
      "a query", 1, txn);
  EXPECT_EQ(param1.len, 1);
  EXPECT_EQ(*param1.buf, 'a');

  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableWithName("department_table")
                ->GetSchema()
                ->GetColumn(1)
                .GetName(),
            "name");
  EXPECT_EQ(time_stamp, 5);
  // We remove these tests so people can add new catalogs without breaking this
  // test...
  // 3 + 4
  // EXPECT_EQ(catalog::Catalog::GetInstance()
  //               ->GetDatabaseWithName("pg_catalog")
  //               ->GetTableWithName("pg_table")
  //               ->GetTupleCount(),
  //           11);
  // // 6 + pg_database(2) + pg_table(3) + pg_attribute(7) + pg_index(6)
  // EXPECT_EQ(catalog::Catalog::GetInstance()
  //               ->GetDatabaseWithName("pg_catalog")
  //               ->GetTableWithName("pg_attribute")
  //               ->GetTupleCount(),
  //           57);
  // // pg_catalog + EMP_DB
  // EXPECT_EQ(catalog::Catalog::GetInstance()
  //               ->GetDatabaseWithName("pg_catalog")
  //               ->GetTableWithName("pg_database")
  //               ->GetTupleCount(),
  //           2);
  // // 3 + pg_index(3) + pg_attribute(3) + pg_table(3) + pg_database(2)
  // EXPECT_EQ(catalog::Catalog::GetInstance()
  //               ->GetDatabaseWithName("pg_catalog")
  //               ->GetTableWithName("pg_index")
  //               ->GetTupleCount(),
  //           18);
  // EXPECT_EQ(catalog::Catalog::GetInstance()
  //               ->GetDatabaseWithName("pg_catalog")
  //               ->GetTableWithName("pg_table")
  //               ->GetSchema()
  //               ->GetLength(),
  //           72);
}

TEST_F(CatalogTests, DroppingTable) {
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableCount(),
            3);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  oid_t database_oid =
      catalog::Catalog::GetInstance()->GetDatabaseWithName("EMP_DB")->GetOid();
  catalog::Catalog::GetInstance()->DropTable("EMP_DB", "department_table", txn);

  oid_t department_table_oid =
      catalog::TableCatalog::GetInstance()->GetTableOid("department_table",
                                                        database_oid, txn);
  txn_manager.CommitTransaction(txn);
  //  catalog::Catalog::GetInstance()->PrintCatalogs();
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableCount(),
            2);

  EXPECT_EQ(department_table_oid, INVALID_OID);

  // Try to drop again
  txn = txn_manager.BeginTransaction();
  ResultType result = catalog::Catalog::GetInstance()->DropTable(
      "EMP_DB", "department_table", txn);
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableCount(),
            2);

  EXPECT_EQ(result, ResultType::FAILURE);

  // Drop a table that does not exist
  txn = txn_manager.BeginTransaction();
  result =
      catalog::Catalog::GetInstance()->DropTable("EMP_DB", "void_table", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(catalog::Catalog::GetInstance()
                ->GetDatabaseWithName("EMP_DB")
                ->GetTableCount(),
            2);
  EXPECT_EQ(result, ResultType::FAILURE);

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

}  // End test namespace
}  // End peloton namespace
