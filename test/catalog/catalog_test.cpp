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

#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/database_metrics_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/layout_catalog.h"
#include "catalog/query_metrics_catalog.h"
#include "catalog/system_catalogs.h"
#include "catalog/table_catalog.h"
#include "common/harness.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"
#include "storage/storage_manager.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CatalogTests : public PelotonTest {};

TEST_F(CatalogTests, BootstrappingCatalog) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  EXPECT_EQ(1, storage::StorageManager::GetInstance()->GetDatabaseCount());
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  storage::Database *database =
      catalog->GetDatabaseWithName(CATALOG_DATABASE_NAME, txn);
  // Check database metric table
  storage::DataTable *db_metric_table =
      catalog->GetTableWithName(CATALOG_DATABASE_NAME, CATALOG_SCHEMA_NAME,
                                DATABASE_METRICS_CATALOG_NAME, txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_NE(nullptr, database);
  EXPECT_NE(nullptr, db_metric_table);
}
//
TEST_F(CatalogTests, CreatingDatabase) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase("emp_db", txn);
  EXPECT_EQ("emp_db", catalog::Catalog::GetInstance()
                          ->GetDatabaseWithName("emp_db", txn)
                          ->GetDBName());
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, CreatingTable) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "id", true);
  id_column.AddConstraint(
      catalog::Constraint(ConstraintType::PRIMARY, "primary_key"));
  auto name_column = catalog::Column(type::TypeId::VARCHAR, 32, "name", true);

  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({id_column, name_column}));
  std::unique_ptr<catalog::Schema> table_schema_2(
      new catalog::Schema({id_column, name_column}));
  std::unique_ptr<catalog::Schema> table_schema_3(
      new catalog::Schema({id_column, name_column}));

  catalog::Catalog::GetInstance()->CreateTable(
      "emp_db", DEFAULT_SCHEMA_NAME, "emp_table", std::move(table_schema), txn);
  catalog::Catalog::GetInstance()->CreateTable("emp_db", DEFAULT_SCHEMA_NAME,
                                               "department_table",
                                               std::move(table_schema_2), txn);
  catalog::Catalog::GetInstance()->CreateTable("emp_db", DEFAULT_SCHEMA_NAME,
                                               "salary_table",
                                               std::move(table_schema_3), txn);
  // insert random tuple into DATABASE_METRICS_CATALOG and check
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());
  catalog::DatabaseMetricsCatalog::GetInstance()->InsertDatabaseMetrics(
      2, 3, 4, 5, pool.get(), txn);

  // inset meaningless tuple into QUERY_METRICS_CATALOG and check
  stats::QueryMetric::QueryParamBuf param;
  param.len = 1;
  param.buf = (unsigned char *)pool->Allocate(1);
  *param.buf = 'a';
  auto database_object =
      catalog::Catalog::GetInstance()->GetDatabaseObject("emp_db", txn);
  catalog::Catalog::GetInstance()
      ->GetSystemCatalogs(database_object->GetDatabaseOid())
      ->GetQueryMetricsCatalog()
      ->InsertQueryMetrics("a query", database_object->GetDatabaseOid(), 1,
                           param, param, param, 1, 1, 1, 1, 1, 1, 1, pool.get(),
                           txn);
  auto param1 = catalog::Catalog::GetInstance()
                    ->GetSystemCatalogs(database_object->GetDatabaseOid())
                    ->GetQueryMetricsCatalog()
                    ->GetParamTypes("a query", txn);
  EXPECT_EQ(1, param1.len);
  EXPECT_EQ('a', *param1.buf);
  // check colum object
  EXPECT_EQ("name", catalog::Catalog::GetInstance()
                        ->GetTableObject("emp_db", DEFAULT_SCHEMA_NAME,
                                         "department_table", txn)
                        ->GetColumnObject(1)
                        ->GetColumnName());
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, TableObject) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      "emp_db", DEFAULT_SCHEMA_NAME, "department_table", txn);

  auto index_objects = table_object->GetIndexObjects();
  auto column_objects = table_object->GetColumnObjects();

  EXPECT_EQ(1, index_objects.size());
  EXPECT_EQ(2, column_objects.size());

  EXPECT_EQ(table_object->GetTableOid(), column_objects[0]->GetTableOid());
  EXPECT_EQ("id", column_objects[0]->GetColumnName());
  EXPECT_EQ(0, column_objects[0]->GetColumnId());
  EXPECT_EQ(0, column_objects[0]->GetColumnOffset());
  EXPECT_EQ(type::TypeId::INTEGER, column_objects[0]->GetColumnType());
  EXPECT_TRUE(column_objects[0]->IsInlined());
  EXPECT_TRUE(column_objects[0]->IsPrimary());
  EXPECT_FALSE(column_objects[0]->IsNotNull());

  EXPECT_EQ(table_object->GetTableOid(), column_objects[1]->GetTableOid());
  EXPECT_EQ("name", column_objects[1]->GetColumnName());
  EXPECT_EQ(1, column_objects[1]->GetColumnId());
  EXPECT_EQ(4, column_objects[1]->GetColumnOffset());
  EXPECT_EQ(type::TypeId::VARCHAR, column_objects[1]->GetColumnType());
  EXPECT_TRUE(column_objects[1]->IsInlined());
  EXPECT_FALSE(column_objects[1]->IsPrimary());
  EXPECT_FALSE(column_objects[1]->IsNotNull());

  // update pg_table SET version_oid = 1 where table_name = department_table
  oid_t department_table_oid = table_object->GetTableOid();
  auto pg_table = catalog::Catalog::GetInstance()
                      ->GetSystemCatalogs(table_object->GetDatabaseOid())
                      ->GetTableCatalog();
  bool update_result = pg_table->UpdateVersionId(1, department_table_oid, txn);
  // get version id after update, invalidate old cache
  table_object = catalog::Catalog::GetInstance()->GetTableObject(
      "emp_db", DEFAULT_SCHEMA_NAME, "department_table", txn);
  uint32_t version_oid = table_object->GetVersionId();
  EXPECT_NE(department_table_oid, INVALID_OID);
  EXPECT_EQ(update_result, true);
  EXPECT_EQ(version_oid, 1);

  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, TestingNamespace) {
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("begin;"));
  // create namespaces emp_ns0 and emp_ns1
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery(
                                     "create database default_database;"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery("create schema emp_ns0;"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery("create schema emp_ns1;"));

  // create emp_table0 and emp_table1 in namespaces
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "create table emp_ns0.emp_table0 (a int, b varchar);"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "create table emp_ns0.emp_table1 (a int, b varchar);"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "create table emp_ns1.emp_table0 (a int, b varchar);"));
  EXPECT_EQ(ResultType::FAILURE,
            TestingSQLUtil::ExecuteSQLQuery(
                "create table emp_ns1.emp_table0 (a int, b varchar);"));

  // insert values into emp_table0
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "insert into emp_ns0.emp_table0 values (1, 'abc');"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "insert into emp_ns0.emp_table0 values (2, 'abc');"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery(
                "insert into emp_ns1.emp_table0 values (1, 'abc');"));

  // select values from emp_table0 and emp_table1
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "select * from emp_ns0.emp_table1;", {});
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "select * from emp_ns0.emp_table0;", {"1|abc", "2|abc"});
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "select * from emp_ns1.emp_table0;", {"1|abc"});
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("commit;"));
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("begin;"));
  EXPECT_EQ(ResultType::FAILURE, TestingSQLUtil::ExecuteSQLQuery(
                                     "select * from emp_ns1.emp_table1;"));
  EXPECT_EQ(ResultType::ABORTED, TestingSQLUtil::ExecuteSQLQuery("commit;"));

  // drop namespace emp_ns0 and emp_ns1
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("begin;"));
  EXPECT_EQ(ResultType::SUCCESS,
            TestingSQLUtil::ExecuteSQLQuery("drop schema emp_ns0;"));
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(
      "select * from emp_ns1.emp_table0;", {"1|abc"});
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("commit;"));
  EXPECT_EQ(ResultType::SUCCESS, TestingSQLUtil::ExecuteSQLQuery("begin;"));
  EXPECT_EQ(ResultType::FAILURE,
            TestingSQLUtil::ExecuteSQLQuery("drop schema emp_ns0;"));
  EXPECT_EQ(ResultType::FAILURE, TestingSQLUtil::ExecuteSQLQuery(
                                     "select * from emp_ns0.emp_table1;"));
  EXPECT_EQ(ResultType::ABORTED, TestingSQLUtil::ExecuteSQLQuery("commit;"));
}

TEST_F(CatalogTests, DroppingTable) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  // NOTE: everytime we create a database, there will be 9 catalog tables
  // inside. Additionally, we create 3 tables for the test.
  oid_t expected_table_count = CATALOG_TABLES_COUNT + 3;
  EXPECT_EQ(
      expected_table_count,
      (int)catalog->GetDatabaseObject("emp_db", txn)->GetTableObjects().size());
  auto database_object =
      catalog::Catalog::GetInstance()->GetDatabaseObject("emp_db", txn);
  EXPECT_NE(nullptr, database_object);
  catalog::Catalog::GetInstance()->DropTable("emp_db", DEFAULT_SCHEMA_NAME,
                                             "department_table", txn);

  database_object =
      catalog::Catalog::GetInstance()->GetDatabaseObject("emp_db", txn);
  EXPECT_NE(nullptr, database_object);
  auto department_table_object =
      database_object->GetTableObject("department_table", DEFAULT_SCHEMA_NAME);
  // Decrement expected_table_count to account for the dropped table.
  expected_table_count--;
  EXPECT_EQ(
      expected_table_count,
      (int)catalog->GetDatabaseObject("emp_db", txn)->GetTableObjects().size());
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(nullptr, department_table_object);

  // Try to drop again
  txn = txn_manager.BeginTransaction();
  EXPECT_THROW(catalog::Catalog::GetInstance()->DropTable(
                   "emp_db", DEFAULT_SCHEMA_NAME, "department_table", txn),
               CatalogException);
  EXPECT_EQ(
      expected_table_count,
      (int)catalog->GetDatabaseObject("emp_db", txn)->GetTableObjects().size());
  txn_manager.CommitTransaction(txn);

  // Drop a table that does not exist
  txn = txn_manager.BeginTransaction();
  EXPECT_THROW(catalog::Catalog::GetInstance()->DropTable(
                   "emp_db", DEFAULT_SCHEMA_NAME, "void_table", txn),
               CatalogException);
  EXPECT_EQ(
      expected_table_count,
      (int)catalog->GetDatabaseObject("emp_db", txn)->GetTableObjects().size());
  txn_manager.CommitTransaction(txn);

  // Drop the other table
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropTable("emp_db", DEFAULT_SCHEMA_NAME,
                                             "emp_table", txn);
  // Account for the dropped table.
  expected_table_count--;
  EXPECT_EQ(
      expected_table_count,
      (int)catalog->GetDatabaseObject("emp_db", txn)->GetTableObjects().size());
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, DroppingDatabase) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName("emp_db", txn);

  EXPECT_THROW(
      catalog::Catalog::GetInstance()->GetDatabaseWithName("emp_db", txn),
      CatalogException);
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, DroppingCatalog) {
  auto catalog = catalog::Catalog::GetInstance();
  EXPECT_NE(nullptr, catalog);
}

TEST_F(CatalogTests, LayoutCatalogTest) {
  // This test creates a table, changes its layout.
  // Create another additional layout.
  // Ensure that default is not changed.
  // Drops layout and verifies that the default_layout is reset.
  // It also queries pg_layout to ensure that the entry is removed.

  auto db_name = "temp_db";
  auto table_name = "temp_table";
  auto catalog = catalog::Catalog::GetInstance();
  // Create database.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_EQ(ResultType::SUCCESS, catalog->CreateDatabase(db_name, txn));

  // Create table.
  auto val0 = catalog::Column(type::TypeId::INTEGER,
                              type::Type::GetTypeSize(type::TypeId::INTEGER),
                              "val0", true);
  auto val1 = catalog::Column(type::TypeId::INTEGER,
                              type::Type::GetTypeSize(type::TypeId::INTEGER),
                              "val1", true);
  auto val2 = catalog::Column(type::TypeId::INTEGER,
                              type::Type::GetTypeSize(type::TypeId::INTEGER),
                              "val2", true);
  auto val3 = catalog::Column(type::TypeId::INTEGER,
                              type::Type::GetTypeSize(type::TypeId::INTEGER),
                              "val3", true);
  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({val0, val1, val2, val3}));
  EXPECT_EQ(ResultType::SUCCESS,
            catalog->CreateTable(db_name, DEFAULT_SCHEMA_NAME, table_name,
                                 std::move(table_schema), txn));
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  auto database_oid =
      catalog->GetDatabaseObject(db_name, txn)->GetDatabaseOid();
  auto table_object =
      catalog->GetTableObject(db_name, DEFAULT_SCHEMA_NAME, table_name, txn);
  auto table_oid = table_object->GetTableOid();
  auto table =
      catalog->GetTableWithName(db_name, DEFAULT_SCHEMA_NAME, table_name, txn);
  txn_manager.CommitTransaction(txn);

  // Change default layout.

  std::map<oid_t, std::pair<oid_t, oid_t>> default_map;
  default_map[0] = std::make_pair(0, 0);
  default_map[1] = std::make_pair(0, 1);
  default_map[2] = std::make_pair(1, 0);
  default_map[3] = std::make_pair(1, 1);

  txn = txn_manager.BeginTransaction();
  auto default_layout =
      catalog->CreateDefaultLayout(database_oid, table_oid, default_map, txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_NE(nullptr, default_layout);

  // Create additional layout.
  std::map<oid_t, std::pair<oid_t, oid_t>> non_default_map;
  non_default_map[0] = std::make_pair(0, 0);
  non_default_map[1] = std::make_pair(0, 1);
  non_default_map[2] = std::make_pair(1, 0);
  non_default_map[3] = std::make_pair(1, 1);

  txn = txn_manager.BeginTransaction();
  auto other_layout =
      catalog->CreateLayout(database_oid, table_oid, non_default_map, txn);
  txn_manager.CommitTransaction(txn);

  // Check that the default layout is still the same.
  EXPECT_NE(*other_layout.get(), table->GetDefaultLayout());

  // Drop the default layout.
  auto default_layout_oid = default_layout->GetOid();
  txn = txn_manager.BeginTransaction();
  EXPECT_EQ(ResultType::SUCCESS, catalog->DropLayout(database_oid, table_oid,
                                                     default_layout_oid, txn));
  txn_manager.CommitTransaction(txn);

  // Check that default layout is reset and set to row_store.
  EXPECT_NE(*default_layout.get(), table->GetDefaultLayout());
  EXPECT_TRUE(table->GetDefaultLayout().IsRowStore());

  // Query pg_layout to ensure that the entry is dropped
  txn = txn_manager.BeginTransaction();
  auto pg_layout = catalog->GetSystemCatalogs(database_oid)->GetLayoutCatalog();
  EXPECT_EQ(nullptr,
            pg_layout->GetLayoutWithOid(table_oid, default_layout_oid, txn));

  // The additional layout must be present in pg_layout
  auto other_layout_oid = other_layout->GetOid();
  EXPECT_EQ(
      *(other_layout.get()),
      *(pg_layout->GetLayoutWithOid(table_oid, other_layout_oid, txn).get()));
  txn_manager.CommitTransaction(txn);

  // Drop database
  txn = txn_manager.BeginTransaction();
  catalog->DropDatabaseWithName(db_name, txn);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
