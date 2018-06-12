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
  void CreateTable_TypeA(const std::string &db_name,
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
    catalog_->CreateTable(db_name, DEFAULT_SCHEMA_NAME, table_name,
                          std::move(table_schema), txn);
    txn_manager_->CommitTransaction(txn);
  }

  /**
   * @brief Create a new table with schema (a INT, b INT, c INT).
   */
  void CreateTable_TypeB(const std::string &db_name,
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
    catalog_->CreateTable(db_name, DEFAULT_SCHEMA_NAME, table_name,
                          std::move(table_schema), txn);
    txn_manager_->CommitTransaction(txn);
  }

  /**
   * @brief Create two indexes on columns (a, b) and (b, c), respectively
   */
  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>>
  CreateIndex_TypeA(const std::string &db_name, const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    const auto db_obj = catalog_->GetDatabaseWithName(db_name, txn);
    const auto db_oid = db_obj->GetOid();
    const auto table_oid = catalog_->GetDatabaseObject(db_name, txn)
                               ->GetTableObject(table_name, DEFAULT_SCHEMA_NAME)
                               ->GetTableOid();
    const auto table_obj = db_obj->GetTableWithOid(table_oid);

    std::vector<std::shared_ptr<brain::HypotheticalIndexObject>> result;

    auto col_a = table_obj->GetSchema()->GetColumnID("a");
    auto col_b = table_obj->GetSchema()->GetColumnID("b");
    auto col_c = table_obj->GetSchema()->GetColumnID("c");
    std::vector<oid_t> index_a_b = {col_a, col_b};
    std::vector<oid_t> index_b_c = {col_b, col_c};

    catalog_->CreateIndex(db_name, DEFAULT_SCHEMA_NAME, table_name, index_a_b,
                          "index_a_b", false, IndexType::BWTREE, txn);
    catalog_->CreateIndex(db_name, DEFAULT_SCHEMA_NAME, table_name, index_b_c,
                          "index_b_c", false, IndexType::BWTREE, txn);

    result.push_back(std::make_shared<brain::HypotheticalIndexObject>(
        db_oid, table_oid, index_a_b));
    result.push_back(std::make_shared<brain::HypotheticalIndexObject>(
        db_oid, table_oid, index_b_c));

    txn_manager_->CommitTransaction(txn);

    return result;
  }

  /**
   * @brief: Get the OID of a table by its name
   */
  oid_t GetTableOid(const std::string &db_name, const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    const auto table_oid = catalog_->GetDatabaseObject(db_name, txn)
                               ->GetTableObject(table_name, DEFAULT_SCHEMA_NAME)
                               ->GetTableOid();
    txn_manager_->CommitTransaction(txn);
    return table_oid;
  }

  /**
   * @brief Create one index on columns (a, c)
   */
  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>>
  CreateIndex_TypeB(const std::string &db_name, const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    const auto db_obj = catalog_->GetDatabaseWithName(db_name, txn);
    const auto db_oid = db_obj->GetOid();
    const auto table_oid = catalog_->GetDatabaseObject(db_name, txn)
                               ->GetTableObject(table_name, DEFAULT_SCHEMA_NAME)
                               ->GetTableOid();
    const auto table_obj = db_obj->GetTableWithOid(table_oid);
    std::vector<std::shared_ptr<brain::HypotheticalIndexObject>> result;

    auto col_a = table_obj->GetSchema()->GetColumnID("a");
    auto col_c = table_obj->GetSchema()->GetColumnID("c");
    std::vector<oid_t> index_a_c = {col_a, col_c};

    catalog_->CreateIndex(db_name, DEFAULT_SCHEMA_NAME, table_name, index_a_c,
                          "index_a_c", false, IndexType::BWTREE, txn);

    result.push_back(std::make_shared<brain::HypotheticalIndexObject>(
        db_oid, table_oid, index_a_c));

    txn_manager_->CommitTransaction(txn);

    return result;
  }

  void DropTable(const std::string &db_name, const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    catalog_->DropTable(db_name, DEFAULT_SCHEMA_NAME, table_name, txn);
    txn_manager_->CommitTransaction(txn);
  }

  void DropDatabase(const std::string &db_name) {
    auto txn = txn_manager_->BeginTransaction();
    catalog_->DropDatabaseWithName(db_name, txn);
    txn_manager_->CommitTransaction(txn);
  }

  std::shared_ptr<brain::HypotheticalIndexObject>
  GetHypotheticalIndexObjectFromString(
      const std::string &db_name, const std::string &table_name,
      const std::vector<std::string> &columns) {
    auto txn = txn_manager_->BeginTransaction();
    const auto db_obj = catalog_->GetDatabaseWithName(db_name, txn);
    const auto db_oid = db_obj->GetOid();
    const auto table_oid = catalog_->GetDatabaseObject(db_name, txn)
                               ->GetTableObject(table_name, DEFAULT_SCHEMA_NAME)
                               ->GetTableOid();
    const auto table_obj = db_obj->GetTableWithOid(table_oid);
    std::vector<oid_t> col_oids;
    for (const auto &col : columns) {
      col_oids.push_back(table_obj->GetSchema()->GetColumnID(col));
    }
    txn_manager_->CommitTransaction(txn);

    return std::make_shared<brain::HypotheticalIndexObject>(db_oid, table_oid,
                                                            col_oids);
  }

 private:
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;
};

TEST_F(CompressedIdxConfigTest, CompressedRepresentationTest) {
  /**This test checks for correctness of the compressed container
   * representation*/
  std::string database_name = DEFAULT_DB_NAME;
  std::string table_name_1 = "dummy_table_1";
  std::string table_name_2 = "dummy_table_2";
  std::string table_name_3 = "dummy_table_3";

  // We build a DB with 2 tables, each having 3 columns
  CreateDatabase(database_name);

  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);

  CreateTable_TypeA(database_name, table_name_1);
  CreateTable_TypeB(database_name, table_name_2);
  CreateTable_TypeB(database_name, table_name_3);

  // create index on (a1, b1) and (b1, c1)
  auto idx_objs = CreateIndex_TypeA(database_name, table_name_1);
  // create index on (a2, c2)
  auto idx_objs_B = CreateIndex_TypeB(database_name, table_name_2);
  // No index on table 3
  // Put everything in the vector of index objects
  idx_objs.insert(idx_objs.end(), idx_objs_B.begin(), idx_objs_B.end());

  auto comp_idx_config =
      brain::CompressedIndexConfigContainer(database_name, ignore_table_oids);
  LOG_DEBUG("bitset: %s", comp_idx_config.ToString().c_str());
  EXPECT_EQ(comp_idx_config.GetConfigurationCount(), 48);
  // 2 created + PK index being created by default
  EXPECT_EQ(
      comp_idx_config.GetNumIndexes(GetTableOid(database_name, table_name_1)),
      3);
  // 1 created
  EXPECT_EQ(
      comp_idx_config.GetNumIndexes(GetTableOid(database_name, table_name_2)),
      1);
  // No index created
  EXPECT_TRUE(
      comp_idx_config.EmptyConfig(GetTableOid(database_name, table_name_3)));

  // Now check that bitset positions exactly align with Indexes present
  for (const auto &idx_obj : idx_objs) {
    size_t global_offset = comp_idx_config.GetGlobalOffset(idx_obj);
    const auto new_idx_obj = comp_idx_config.GetIndex(global_offset);
    EXPECT_TRUE(comp_idx_config.IsSet(idx_obj));
    std::set<oid_t> idx_obj_cols(idx_obj->column_oids.begin(),
                                 idx_obj->column_oids.end());
    std::set<oid_t> new_idx_obj_cols(new_idx_obj->column_oids.begin(),
                                     new_idx_obj->column_oids.end());
    EXPECT_EQ(idx_obj->db_oid, new_idx_obj->db_oid);
    EXPECT_EQ(idx_obj->table_oid, new_idx_obj->table_oid);
    EXPECT_EQ(idx_obj_cols, new_idx_obj_cols);
  }
  DropDatabase(database_name);
}

TEST_F(CompressedIdxConfigTest, AddDropCandidatesTest) {
  std::string database_name = DEFAULT_DB_NAME;
  std::string table_name_1 = "dummy_table_1";

  // We build a DB with 1 table, having 3 columns (a INT, b INT, c INT). b is
  // PRIMARY KEY.
  CreateDatabase(database_name);
  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);
  CreateTable_TypeA(database_name, table_name_1);

  // create index on (a1, b1) and (b1, c1)
  auto idx_objs = CreateIndex_TypeA(database_name, table_name_1);

  auto comp_idx_config =
      brain::CompressedIndexConfigContainer(database_name, ignore_table_oids);
  LOG_DEBUG("bitset: %s", comp_idx_config.ToString().c_str());
  // Total configuration = total number of permutations: 1 * 3! + 3 * 2! + 3 *
  // 1! + 1 = 16
  EXPECT_EQ(comp_idx_config.GetConfigurationCount(), 16);
  // 2 created + PK index being created by default
  EXPECT_FALSE(
      comp_idx_config.EmptyConfig(GetTableOid(database_name, table_name_1)));
  EXPECT_EQ(
      comp_idx_config.GetNumIndexes(GetTableOid(database_name, table_name_1)),
      3);

  std::string query_string =
      "UPDATE dummy_table_1 SET a = 0 WHERE b = 1 AND c = 2;";
  boost::dynamic_bitset<> drop_candidates, add_candidates_single,
      add_candidates_multiple;
  brain::CompressedIndexConfigUtil::DropCandidates(
      comp_idx_config, query_string, drop_candidates);
  brain::CompressedIndexConfigUtil::AddCandidates(
      comp_idx_config, query_string, add_candidates_single, true, 0);
  brain::CompressedIndexConfigUtil::AddCandidates(
      comp_idx_config, query_string, add_candidates_multiple, false, 2);

  auto index_empty =
      GetHypotheticalIndexObjectFromString(database_name, table_name_1, {});
  auto index_b =
      GetHypotheticalIndexObjectFromString(database_name, table_name_1, {"b"});
  auto index_c =
      GetHypotheticalIndexObjectFromString(database_name, table_name_1, {"c"});
  auto index_b_c = GetHypotheticalIndexObjectFromString(
      database_name, table_name_1, {"b", "c"});
  auto index_c_b = GetHypotheticalIndexObjectFromString(
      database_name, table_name_1, {"c", "b"});

  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>>
      add_expect_indexes_single = {index_b, index_c};
  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>>
      add_expect_indexes_multiple = {index_empty, index_b, index_c, index_b_c,
                                     index_c_b};
  // since b is primary key, we will ignore index {a, b}
  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>>
      drop_expect_indexes = {};

  auto add_expect_bitset_single =
      brain::CompressedIndexConfigUtil::GenerateBitSet(
          comp_idx_config, add_expect_indexes_single);
  auto add_expect_bitset_multiple =
      brain::CompressedIndexConfigUtil::GenerateBitSet(
          comp_idx_config, add_expect_indexes_multiple);
  auto drop_expect_bitset = brain::CompressedIndexConfigUtil::GenerateBitSet(
      comp_idx_config, drop_expect_indexes);

  EXPECT_EQ(*add_expect_bitset_single, add_candidates_single);
  EXPECT_EQ(*add_expect_bitset_multiple, add_candidates_multiple);
  EXPECT_EQ(*drop_expect_bitset, drop_candidates);

  DropDatabase(database_name);
}

}  // namespace test
}  // namespace peloton
