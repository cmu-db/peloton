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
#include "brain/indextune/compressed_index_config.h"
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
 public:
  RLFrameworkTest()
      : catalog_{catalog::Catalog::GetInstance()},
        txn_manager_{&concurrency::TransactionManagerFactory::GetInstance()} {}

  // Create a new database
  void CreateDatabase(const std::string &db_name) {
    auto txn = txn_manager_->BeginTransaction();
    catalog_->CreateDatabase(db_name, txn);
    txn_manager_->CommitTransaction(txn);
  }

  // Create a new table with schema (a INT, b INT, c INT).
  void CreateTable(const std::string &db_name, const std::string &table_name) {
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
    catalog_->CreateTable(db_name, table_name, std::move(table_schema), txn);
    txn_manager_->CommitTransaction(txn);
  }

  void CreateIndex_A(const std::string &db_name,
                     const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    const auto db_obj = catalog_->GetDatabaseWithName(db_name, txn);
    const auto table_obj = db_obj->GetTableWithName(table_name);

    auto col_a = table_obj->GetSchema()->GetColumnID("a");
    auto col_b = table_obj->GetSchema()->GetColumnID("b");
    auto col_c = table_obj->GetSchema()->GetColumnID("c");
    std::vector<oid_t> index_a_b = {col_a, col_b};
    std::vector<oid_t> index_b_c = {col_b, col_c};

    catalog_->CreateIndex(db_name, table_name, index_a_b, "index_a_b", false,
                          IndexType::BWTREE, txn);
    catalog_->CreateIndex(db_name, table_name, index_b_c, "index_b_c", false,
                          IndexType::BWTREE, txn);

    txn_manager_->CommitTransaction(txn);
  }

  void CreateIndex_B(const std::string &db_name,
                     const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    const auto db_obj = catalog_->GetDatabaseWithName(db_name, txn);
    const auto table_obj = db_obj->GetTableWithName(table_name);

    auto col_a = table_obj->GetSchema()->GetColumnID("a");
    auto col_c = table_obj->GetSchema()->GetColumnID("c");
    std::vector<oid_t> index_a_c = {col_a, col_c};

    catalog_->CreateIndex(db_name, table_name, index_a_c, "index_a_c", false,
                          IndexType::BWTREE, txn);

    txn_manager_->CommitTransaction(txn);
  }

  void DropTable(const std::string &db_name, const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    catalog_->DropTable(db_name, table_name, txn);
    txn_manager_->CommitTransaction(txn);
  }

  void DropDatabase(const std::string &db_name) {
    auto txn = txn_manager_->BeginTransaction();
    catalog_->DropDatabaseWithName(db_name, txn);
    txn_manager_->CommitTransaction(txn);
  }

 private:
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;
};

TEST_F(RLFrameworkTest, BasicTest) {
  std::string database_name = DEFAULT_DB_NAME;
  std::string table_name_1 = "dummy_table_1";
  std::string table_name_2 = "dummy_table_2";

  CreateDatabase(database_name);
  CreateTable(database_name, table_name_1);
  CreateTable(database_name, table_name_2);

  // create index on (a, b) and (b, c)
  CreateIndex_A(database_name, table_name_1);
  // create index on (a, c)
  CreateIndex_B(database_name, table_name_2);

  auto comp_idx_config = std::unique_ptr<brain::CompressedIndexConfiguration>(
      new brain::CompressedIndexConfiguration(database_name));

  auto cur_bit_set = comp_idx_config->GetCurrentIndexConfig();
  std::string output;
  boost::to_string(*cur_bit_set, output);
  LOG_DEBUG("bitset: %s", output.c_str());
}

}  // namespace test
}  // namespace peloton
