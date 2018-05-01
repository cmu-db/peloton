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

  /**
   * @brief Create a new database
   */
  void CreateDatabase(const std::string &db_name) {
    auto txn = txn_manager_->BeginTransaction();
    catalog_->CreateDatabase(db_name, txn);
    txn_manager_->CommitTransaction(txn);
  }

  /**
   * @brief Create a new table with schema (a INT, b INT, c INT).
   */
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

  /**
   * @brief Create two indexes on columns (a, b) and (b, c), respectively
   */
  std::vector<std::shared_ptr<brain::IndexObject>> CreateIndex_A(
      const std::string &db_name, const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    const auto db_obj = catalog_->GetDatabaseWithName(db_name, txn);
    const auto db_oid = db_obj->GetOid();
    const auto table_obj = db_obj->GetTableWithName(table_name);
    const auto table_oid = table_obj->GetOid();
    std::vector<std::shared_ptr<brain::IndexObject>> result;

    auto col_a = table_obj->GetSchema()->GetColumnID("a");
    auto col_b = table_obj->GetSchema()->GetColumnID("b");
    auto col_c = table_obj->GetSchema()->GetColumnID("c");
    std::vector<oid_t> index_a_b = {col_a, col_b};
    std::vector<oid_t> index_b_c = {col_b, col_c};

    catalog_->CreateIndex(db_name, table_name, index_a_b, "index_a_b", false,
                          IndexType::BWTREE, txn);
    catalog_->CreateIndex(db_name, table_name, index_b_c, "index_b_c", false,
                          IndexType::BWTREE, txn);

    result.push_back(
        std::make_shared<brain::IndexObject>(db_oid, table_oid, index_a_b));
    result.push_back(
        std::make_shared<brain::IndexObject>(db_oid, table_oid, index_b_c));

    txn_manager_->CommitTransaction(txn);

    return result;
  }

  /**
   * @brief Create one index on columns (a, c)
   */
  std::vector<std::shared_ptr<brain::IndexObject>> CreateIndex_B(
      const std::string &db_name, const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    const auto db_obj = catalog_->GetDatabaseWithName(db_name, txn);
    const auto db_oid = db_obj->GetOid();
    const auto table_obj = db_obj->GetTableWithName(table_name);
    const auto table_oid = table_obj->GetOid();
    std::vector<std::shared_ptr<brain::IndexObject>> result;

    auto col_a = table_obj->GetSchema()->GetColumnID("a");
    auto col_c = table_obj->GetSchema()->GetColumnID("c");
    std::vector<oid_t> index_a_c = {col_a, col_c};

    catalog_->CreateIndex(db_name, table_name, index_a_c, "index_a_c", false,
                          IndexType::BWTREE, txn);

    result.push_back(
        std::make_shared<brain::IndexObject>(db_oid, table_oid, index_a_c));

    txn_manager_->CommitTransaction(txn);

    return result;
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
  auto idx_objs = CreateIndex_A(database_name, table_name_1);
  // create index on (a, c)
  auto idx_objs_B = CreateIndex_B(database_name, table_name_2);

  idx_objs.insert(idx_objs.end(), idx_objs_B.begin(), idx_objs_B.end());

  auto comp_idx_config = std::unique_ptr<brain::CompressedIndexConfiguration>(
      new brain::CompressedIndexConfiguration(database_name));

  auto cur_bit_set = comp_idx_config->GetCurrentIndexConfig();
  std::string output;
  boost::to_string(*cur_bit_set, output);
  LOG_DEBUG("bitset: %s", output.c_str());

  for (const auto &idx_obj : idx_objs) {
    size_t global_offset = comp_idx_config->GetGlobalOffset(idx_obj);
    const auto new_idx_obj = comp_idx_config->GetIndex(global_offset);
    EXPECT_TRUE(comp_idx_config->IsSet(idx_obj));
    EXPECT_EQ(*idx_obj, *new_idx_obj);
  }
}

}  // namespace test
}  // namespace peloton
