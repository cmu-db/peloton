//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compressed_idx_config_test.cpp
//
// Identification: test/brain/compressed_idx_config_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection.h"
#include "brain/indextune/compressed_index_config.h"
#include "brain/indextune/compressed_index_config_util.h"
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

class CompressedIdxConfigTest : public PelotonTest {
 public:
  CompressedIdxConfigTest()
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
   * @brief Create a new table with schema (a INT, b INT, c INT). b is PRIMARY
   * KEY.
   */
  void CreateTable_A(const std::string &db_name,
                     const std::string &table_name) {
    auto a_column = catalog::Column(
        type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
        "a", true);

    auto b_column = catalog::Column(
        type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
        "b", true);
    auto b_primary = catalog::Constraint(ConstraintType::PRIMARY, "b_primary");
    b_column.AddConstraint(b_primary);

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
   * @brief Create a new table with schema (a INT, b INT, c INT).
   */
  void CreateTable_B(const std::string &db_name,
                     const std::string &table_name) {
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
  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>> CreateIndex_A(
      const std::string &db_name, const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    const auto db_obj = catalog_->GetDatabaseWithName(db_name, txn);
    const auto db_oid = db_obj->GetOid();
    const auto table_obj = db_obj->GetTableWithName(table_name);
    const auto table_oid = table_obj->GetOid();
    std::vector<std::shared_ptr<brain::HypotheticalIndexObject>> result;

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
        std::make_shared<brain::HypotheticalIndexObject>(db_oid, table_oid, index_a_b));
    result.push_back(
        std::make_shared<brain::HypotheticalIndexObject>(db_oid, table_oid, index_b_c));

    txn_manager_->CommitTransaction(txn);

    return result;
  }

  /**
   * @brief Create one index on columns (a, c)
   */
  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>> CreateIndex_B(
      const std::string &db_name, const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    const auto db_obj = catalog_->GetDatabaseWithName(db_name, txn);
    const auto db_oid = db_obj->GetOid();
    const auto table_obj = db_obj->GetTableWithName(table_name);
    const auto table_oid = table_obj->GetOid();
    std::vector<std::shared_ptr<brain::HypotheticalIndexObject>> result;

    auto col_a = table_obj->GetSchema()->GetColumnID("a");
    auto col_c = table_obj->GetSchema()->GetColumnID("c");
    std::vector<oid_t> index_a_c = {col_a, col_c};

    catalog_->CreateIndex(db_name, table_name, index_a_c, "index_a_c", false,
                          IndexType::BWTREE, txn);

    result.push_back(
        std::make_shared<brain::HypotheticalIndexObject>(db_oid, table_oid, index_a_c));

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

  std::shared_ptr<brain::HypotheticalIndexObject> GetHypotheticalIndexObjectFromString(
      const std::string &database_name, const std::string &table_name,
      const std::vector<std::string> &columns) {
    auto txn = txn_manager_->BeginTransaction();
    const auto db_obj = catalog_->GetDatabaseWithName(database_name, txn);
    const auto db_oid = db_obj->GetOid();
    const auto table_obj = db_obj->GetTableWithName(table_name);
    const auto table_oid = table_obj->GetOid();
    std::vector<oid_t> col_oids;
    for (const auto &col : columns) {
      col_oids.push_back(table_obj->GetSchema()->GetColumnID(col));
    }
    txn_manager_->CommitTransaction(txn);

    return std::make_shared<brain::HypotheticalIndexObject>(db_oid, table_oid, col_oids);
  }

 private:
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;
};

TEST_F(CompressedIdxConfigTest, BasicTest) {
  std::string database_name = DEFAULT_DB_NAME;
  std::string table_name_1 = "dummy_table_1";
  std::string table_name_2 = "dummy_table_2";

  // We build a DB with 2 tables, each having 3 columns
  CreateDatabase(database_name);
  CreateTable_A(database_name, table_name_1);
  CreateTable_B(database_name, table_name_2);

  // create index on (a1, b1) and (b1, c1)
  auto idx_objs = CreateIndex_A(database_name, table_name_1);
  // create index on (a2, c2)
  auto idx_objs_B = CreateIndex_B(database_name, table_name_2);
  // Put everything in the vector of index objects
  idx_objs.insert(idx_objs.end(), idx_objs_B.begin(), idx_objs_B.end());

  auto comp_idx_config = brain::CompressedIndexConfigContainer(database_name);
  // We expect 2**3 possible configurations
  EXPECT_EQ(comp_idx_config.GetConfigurationCount(), 16);

  LOG_DEBUG("bitset: %s", comp_idx_config.ToString().c_str());

  for (const auto &idx_obj : idx_objs) {
    size_t global_offset = comp_idx_config.GetGlobalOffset(idx_obj);
    const auto new_idx_obj = comp_idx_config.GetIndex(global_offset);
    EXPECT_TRUE(comp_idx_config.IsSet(idx_obj));
    EXPECT_EQ(*idx_obj, *new_idx_obj);
  }

  std::string query_string =
      "UPDATE dummy_table_1 SET a = 0 WHERE b = 1 AND c = 2;";
  boost::dynamic_bitset<> drop_candidates, add_candidates;
  brain::CompressedIndexConfigUtil::DropCandidates(
      comp_idx_config, query_string, drop_candidates);
  brain::CompressedIndexConfigUtil::AddCandidates(comp_idx_config, query_string,
                                                  add_candidates);

  auto index_empty = GetHypotheticalIndexObjectFromString(database_name, table_name_1, {});
  auto index_b = GetHypotheticalIndexObjectFromString(database_name, table_name_1, {"b"});
  auto index_a_b =
      GetHypotheticalIndexObjectFromString(database_name, table_name_1, {"a", "b"});
  auto index_b_c =
      GetHypotheticalIndexObjectFromString(database_name, table_name_1, {"b", "c"});

  // we should have prefix closure: {}, {b}, {b, c}
  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>> add_expect_indexes = {
      index_empty, index_b, index_b_c};
  // since b is primary key, we will ignore index {a, b}
  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>> drop_expect_indexes = {};

  auto add_expect_bitset = brain::CompressedIndexConfigUtil::GenerateBitSet(
      comp_idx_config, add_expect_indexes);
  auto drop_expect_bitset = brain::CompressedIndexConfigUtil::GenerateBitSet(
      comp_idx_config, drop_expect_indexes);

  EXPECT_EQ(*add_expect_bitset, add_candidates);
  EXPECT_EQ(*drop_expect_bitset, drop_candidates);
}

}  // namespace test
}  // namespace peloton
