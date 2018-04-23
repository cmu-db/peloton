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
#include "catalog/index_catalog.h"
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
};

TEST_F(RLFrameworkTest, BasicTest) {
  std::string database_name = DEFAULT_DB_NAME;
  std::string table_name_1 = "dummy_table_1";
  std::string table_name_2 = "dummy_table_2";

  CreateDatabase(database_name);
  CreateTable(table_name_1);
  CreateTable(table_name_2);
}

}  // namespace test
}  // namespace peloton
