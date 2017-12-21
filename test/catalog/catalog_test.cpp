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
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/database_metrics_catalog.h"
#include "catalog/query_metrics_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "common/harness.h"
#include "common/logger.h"
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
  txn_manager.CommitTransaction(txn);
  EXPECT_NE(nullptr, database);
  // Check database metric table
  auto db_metric_table =
      database->GetTableWithName(DATABASE_METRICS_CATALOG_NAME);
  EXPECT_NE(nullptr, db_metric_table);
}
//
TEST_F(CatalogTests, CreatingDatabase) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase("EMP_DB", txn);
  auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      CATALOG_DATABASE_NAME, INDEX_CATALOG_NAME, txn);
  auto index_object = table_object->GetIndexObject(INDEX_CATALOG_PKEY_OID);
  std::vector<oid_t> key_attrs = index_object->GetKeyAttrs();

  EXPECT_EQ("EMP_DB", catalog::Catalog::GetInstance()
                          ->GetDatabaseWithName("EMP_DB", txn)
                          ->GetDBName());
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(1, key_attrs.size());
  EXPECT_EQ(0, key_attrs[0]);
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
  //   oid_t time_stamp =
  //       catalog::DatabaseMetricsCatalog::GetInstance()->GetTimeStamp(2, txn);

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
  EXPECT_EQ(1, param1.len);
  EXPECT_EQ('a', *param1.buf);

  EXPECT_EQ("name", catalog::Catalog::GetInstance()
                        ->GetDatabaseWithName("EMP_DB", txn)
                        ->GetTableWithName("department_table")
                        ->GetSchema()
                        ->GetColumn(1)
                        .GetName());
  txn_manager.CommitTransaction(txn);
  // EXPECT_EQ(5, time_stamp);

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

TEST_F(CatalogTests, TableObject) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      "EMP_DB", "department_table", txn);

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

  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, DroppingTable) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto catalog = catalog::Catalog::GetInstance();
  EXPECT_EQ(
      3,
      (int)catalog->GetDatabaseObject("EMP_DB", txn)->GetTableObjects().size());
  auto database_object =
      catalog::Catalog::GetInstance()->GetDatabaseObject("EMP_DB", txn);
  EXPECT_NE(nullptr, database_object);
  catalog::Catalog::GetInstance()->DropTable("EMP_DB", "department_table", txn);

  database_object =
      catalog::Catalog::GetInstance()->GetDatabaseObject("EMP_DB", txn);
  EXPECT_NE(nullptr, database_object);
  auto department_table_object =
      database_object->GetTableObject("department_table");
  //  catalog::Catalog::GetInstance()->PrintCatalogs();
  EXPECT_EQ(
      2,
      (int)catalog->GetDatabaseObject("EMP_DB", txn)->GetTableObjects().size());
  txn_manager.CommitTransaction(txn);

  EXPECT_EQ(nullptr, department_table_object);

  // Try to drop again
  txn = txn_manager.BeginTransaction();
  EXPECT_THROW(catalog::Catalog::GetInstance()->DropTable(
                   "EMP_DB", "department_table", txn),
               CatalogException);

  EXPECT_EQ(
      2,
      (int)catalog->GetDatabaseObject("EMP_DB", txn)->GetTableObjects().size());
  txn_manager.CommitTransaction(txn);

  // Drop a table that does not exist
  txn = txn_manager.BeginTransaction();
  EXPECT_THROW(
      catalog::Catalog::GetInstance()->DropTable("EMP_DB", "void_table", txn),
      CatalogException);
  EXPECT_EQ(
      2,
      (int)catalog->GetDatabaseObject("EMP_DB", txn)->GetTableObjects().size());
  txn_manager.CommitTransaction(txn);

  // Drop the other table
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropTable("EMP_DB", "emp_table", txn);
  EXPECT_EQ(
      1,
      (int)catalog->GetDatabaseObject("EMP_DB", txn)->GetTableObjects().size());
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, DroppingDatabase) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName("EMP_DB", txn);

  EXPECT_THROW(
      catalog::Catalog::GetInstance()->GetDatabaseWithName("EMP_DB", txn),
      CatalogException);
  txn_manager.CommitTransaction(txn);
}

TEST_F(CatalogTests, DroppingCatalog) {
  auto catalog = catalog::Catalog::GetInstance();
  EXPECT_NE(nullptr, catalog);
}

}  // namespace test
}  // namespace peloton
