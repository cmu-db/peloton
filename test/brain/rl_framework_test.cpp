//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rl_framework_test.cpp
//
// Identification: test/brain/rl_framework_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection.h"
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "util/file_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// RL Framework Tests
//===--------------------------------------------------------------------===//

class RLFrameworkTest : public PelotonTest {
 private:
  std::string database_name_;
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;

 public:
  RLFrameworkTest()
      : catalog_{catalog::Catalog::GetInstance()},
        txn_manager_(&concurrency::TransactionManagerFactory::GetInstance()) {
    catalog_->Bootstrap();
  }

  // Create a new database
  void CreateDatabase(const std::string &db_name) {
    database_name_ = db_name;

    auto txn = txn_manager_->BeginTransaction();
    catalog_->CreateDatabase(database_name_, txn);
    txn_manager_->CommitTransaction(txn);
  }

  // Create a new table with schema (a INT, b INT, c INT).
  void CreateTable(const std::string &table_name) {
    auto a_column = catalog::Column(
        type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
        "a", true);
    auto b_column = catalog::Column(
        type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
        "b", true);
    auto c_column = catalog::Column(
        type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
        "c", true);
    std::unique_ptr<catalog::Schema> table_schema(
        new catalog::Schema({a_column, b_column, c_column}));

    auto txn = txn_manager_->BeginTransaction();
    catalog_->CreateTable(database_name_, table_name, std::move(table_schema),
                          txn);
    txn_manager_->CommitTransaction(txn);
  }

  void DropTable(const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    catalog_->DropTable(database_name_, table_name, txn);
    txn_manager_->CommitTransaction(txn);
  }

  void DropDatabase() {
    auto txn = txn_manager_->BeginTransaction();
    catalog_->DropDatabaseWithName(database_name_, txn);
    txn_manager_->CommitTransaction(txn);
  }

  std::vector<std::tuple<oid_t, oid_t, oid_t>> GetAllColumns() {
    std::vector<std::tuple<oid_t, oid_t, oid_t>> result;

    auto txn = txn_manager_->BeginTransaction();

    const auto db_object = catalog_->GetDatabaseObject(database_name_, txn);
    oid_t db_oid = db_object->GetDatabaseOid();
    const auto table_objects = db_object->GetTableObjects();

    for (const auto &it : table_objects) {
      oid_t table_oid = it.first;
      const auto table_obj = it.second;
      const auto column_objects = table_obj->GetColumnObjects();
      for (const auto &col_it : column_objects) {
        oid_t col_oid = col_it.first;
        result.emplace_back(db_oid, table_oid, col_oid);
      }
    }

    txn_manager_->CommitTransaction(txn);

    return result;
  }

  std::vector<std::tuple<oid_t, oid_t, oid_t>> GetAllIndexes() {
    std::vector<std::tuple<oid_t, oid_t, oid_t>> result;

    auto txn = txn_manager_->BeginTransaction();

    const auto db_object = catalog_->GetDatabaseObject(database_name_, txn);
    oid_t db_oid = db_object->GetDatabaseOid();
    const auto table_objects = db_object->GetTableObjects();

    for (const auto &it : table_objects) {
      oid_t table_oid = it.first;
      const auto table_obj = it.second;
      const auto index_objects = table_obj->GetIndexObjects();
      for (const auto &idx_it : index_objects) {
        oid_t idx_oid = idx_it.first;
        result.emplace_back(db_oid, table_oid, idx_oid);
      }
    }

    txn_manager_->CommitTransaction(txn);

    return result;
  }
};

TEST_F(RLFrameworkTest, BasicTest) {
  std::string database_name = DEFAULT_DB_NAME;
  std::string table_name_1 = "dummy_table_1";
  std::string table_name_2 = "dummy_table_2";

  CreateDatabase(database_name);
  CreateTable(table_name_1);
  CreateTable(table_name_2);

  auto all_columns = GetAllColumns();
  LOG_DEBUG("All columns:");
  for (const auto &it : all_columns) {
    LOG_DEBUG("column [%d, %d, %d]", (int)std::get<0>(it), (int)std::get<1>(it), (int)std::get<2>(it));
  }

  auto all_indexes = GetAllIndexes();
  LOG_DEBUG("All indexes:");
  for (const auto &it : all_indexes) {
    LOG_DEBUG("index [%d, %d, %d]", (int)std::get<0>(it), (int)std::get<1>(it), (int)std::get<2>(it));
  }
}

}  // namespace test
}  // namespace peloton
