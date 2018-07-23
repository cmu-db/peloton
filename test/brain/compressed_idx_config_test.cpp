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

#include "brain/testing_index_selection_util.h"
#include "brain/index_selection.h"
#include "brain/indextune/lspi/lspi_common.h"
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

 private:
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;
};

TEST_F(CompressedIdxConfigTest, CompressedRepresentationTest) {
  /**This test checks for correctness of the compressed container
   * representation*/
  std::string database_name = DEFAULT_DB_NAME;
  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  // Initialization
  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);
  auto config = testing_util.GetQueryStringsWorkload(
      index_selection::QueryStringsWorkloadType::MultiTableNoop);

  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
  }

  std::string table_name_1 = table_schemas[0].table_name;
  std::string table_name_2 = table_schemas[1].table_name;
  std::string table_name_3 = table_schemas[2].table_name;


  auto index_ab_t1 = testing_util.CreateHypotheticalIndex(table_name_1, {"a", "b"});
  auto index_bc_t1 = testing_util.CreateHypotheticalIndex(table_name_1, {"b", "c"});
  auto index_ac_t2 = testing_util.CreateHypotheticalIndex(table_name_2, {"a", "c"});

  auto index_objs = {index_ab_t1, index_bc_t1, index_ac_t2};

  for(auto index_obj: index_objs) {
    testing_util.CreateIndex(index_obj);
  }

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
  for (const auto &index_obj : index_objs) {
    size_t global_offset = comp_idx_config.GetGlobalOffset(index_obj);
    const auto new_idx_obj = comp_idx_config.GetIndex(global_offset);
    EXPECT_TRUE(comp_idx_config.IsSet(index_obj));
    std::set<oid_t> idx_obj_cols(index_obj->column_oids.begin(),
                                 index_obj->column_oids.end());
    std::set<oid_t> new_idx_obj_cols(new_idx_obj->column_oids.begin(),
                                     new_idx_obj->column_oids.end());
    EXPECT_EQ(index_obj->db_oid, new_idx_obj->db_oid);
    EXPECT_EQ(index_obj->table_oid, new_idx_obj->table_oid);
    EXPECT_EQ(idx_obj_cols, new_idx_obj_cols);
  }
}

TEST_F(CompressedIdxConfigTest, ConfigEnumerationTest) {

}

TEST_F(CompressedIdxConfigTest, AddSimpleCandidatesTest) {
  std::string database_name = DEFAULT_DB_NAME;
  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  // Initialization
  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);
  auto config = testing_util.GetQueryStringsWorkload(
      index_selection::QueryStringsWorkloadType::SingleTableNoop);

  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
  }

  std::string table_name = table_schemas[0].table_name;

  auto index_ab = testing_util.CreateHypotheticalIndex(table_name, {"a", "b"});
  testing_util.CreateIndex(index_ab);
  auto index_bc = testing_util.CreateHypotheticalIndex(table_name, {"b", "c"});
  testing_util.CreateIndex(index_bc);

  auto comp_idx_config =
      brain::CompressedIndexConfigContainer(database_name, ignore_table_oids);
  LOG_DEBUG("bitset: %s", comp_idx_config.ToString().c_str());
  // Total configuration = total number of permutations: 1 * 3! + 3 * 2! + 3 *
  // 1! + 1 = 16
  EXPECT_EQ(comp_idx_config.GetConfigurationCount(), 16);
  // 2 created + PK index being created by default
  EXPECT_FALSE(
      comp_idx_config.EmptyConfig(GetTableOid(database_name, table_name)));
  EXPECT_EQ(
      comp_idx_config.GetNumIndexes(GetTableOid(database_name, table_name)),
      3);

  std::string query_string = query_strings[0];
  boost::dynamic_bitset<> add_candidates_simple;
  brain::CompressedIndexConfigUtil::AddCandidates(
      comp_idx_config, query_string, add_candidates_simple,
      brain::CandidateSelectionType::Simple);

  auto index_empty = testing_util.CreateHypotheticalIndex(table_name, {});
  auto index_b = testing_util.CreateHypotheticalIndex(table_name, {"b"});
  auto index_c =
      testing_util.CreateHypotheticalIndex(table_name, {"c"});

  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>>
      add_expect_indexes_simple = {index_empty, index_b, index_c};

  auto add_expect_bitset_simple =
      brain::CompressedIndexConfigUtil::GenerateBitSet(
          comp_idx_config, add_expect_indexes_simple);

  EXPECT_EQ(*add_expect_bitset_simple, add_candidates_simple);
}

TEST_F(CompressedIdxConfigTest, AddAutoAdminCandidatesTest) {
  std::string database_name = DEFAULT_DB_NAME;
  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  // Initialization
  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);
  auto config = testing_util.GetQueryStringsWorkload(
      index_selection::QueryStringsWorkloadType::SingleTableNoop);

  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
  }

  std::string table_name = table_schemas[0].table_name;

  auto index_ab = testing_util.CreateHypotheticalIndex(table_name, {"a", "b"});
  testing_util.CreateIndex(index_ab);
  auto index_bc = testing_util.CreateHypotheticalIndex(table_name, {"b", "c"});
  testing_util.CreateIndex(index_bc);

  auto comp_idx_config =
      brain::CompressedIndexConfigContainer(database_name, ignore_table_oids);
  LOG_DEBUG("bitset: %s", comp_idx_config.ToString().c_str());
  // Total configuration = total number of permutations: 1 * 3! + 3 * 2! + 3 *
  // 1! + 1 = 16
  EXPECT_EQ(comp_idx_config.GetConfigurationCount(), 16);
  // 2 created + PK index being created by default
  EXPECT_FALSE(
      comp_idx_config.EmptyConfig(GetTableOid(database_name, table_name)));
  EXPECT_EQ(
      comp_idx_config.GetNumIndexes(GetTableOid(database_name, table_name)),
      3);
  size_t max_index_cols = 2; // multi-column index limit
  size_t enumeration_threshold = 2;  // naive enumeration
  size_t num_indexes = 1; // essentially get all possible indexes
  brain::IndexSelectionKnobs knobs = {max_index_cols, enumeration_threshold,
                                      num_indexes};
  std::string query_string = query_strings[0];
  boost::dynamic_bitset<> add_candidates;
  // TODO(saatviks): Indexes generated seem a bit weird - need to recheck whats happening here
  // When turning up `num_indexes` to 10, this doesnt recommend 1, 2, (1, 2) and (2, 1)?
  // Logs show correct set, but actual returned seem to be from 1 iteration less
  brain::CompressedIndexConfigUtil::AddCandidates(
      comp_idx_config, query_string, add_candidates,
      brain::CandidateSelectionType::AutoAdmin, 0, knobs);

  auto index_empty = testing_util.CreateHypotheticalIndex(table_name, {});
  auto index_b = testing_util.CreateHypotheticalIndex(table_name, {"b"});

  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>>
      add_expect_indexes = {index_empty, index_b};

  auto add_expect_bitset =
      brain::CompressedIndexConfigUtil::GenerateBitSet(
          comp_idx_config, add_expect_indexes);

  EXPECT_EQ(*add_expect_bitset, add_candidates);
}

TEST_F(CompressedIdxConfigTest, AddExhaustiveCandidatesTest) {
  std::string database_name = DEFAULT_DB_NAME;
  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  // Initialization
  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);
  auto config = testing_util.GetQueryStringsWorkload(
      index_selection::QueryStringsWorkloadType::SingleTableNoop);

  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
  }

  std::string table_name = table_schemas[0].table_name;

  auto index_ab = testing_util.CreateHypotheticalIndex(table_name, {"a", "b"});
  testing_util.CreateIndex(index_ab);
  auto index_bc = testing_util.CreateHypotheticalIndex(table_name, {"b", "c"});
  testing_util.CreateIndex(index_bc);

  auto comp_idx_config =
      brain::CompressedIndexConfigContainer(database_name, ignore_table_oids);
  LOG_DEBUG("bitset: %s", comp_idx_config.ToString().c_str());
  // Total configuration = total number of permutations: 1 * 3! + 3 * 2! + 3 *
  // 1! + 1 = 16
  EXPECT_EQ(comp_idx_config.GetConfigurationCount(), 16);
  // 2 created + PK index being created by default
  EXPECT_FALSE(
      comp_idx_config.EmptyConfig(GetTableOid(database_name, table_name)));
  EXPECT_EQ(
      comp_idx_config.GetNumIndexes(GetTableOid(database_name, table_name)),
      3);

  std::string query_string = query_strings[0];
  boost::dynamic_bitset<> add_candidates_exhaustive;
  brain::CompressedIndexConfigUtil::AddCandidates(
      comp_idx_config, query_string, add_candidates_exhaustive,
      brain::CandidateSelectionType::Exhaustive, 2);

  auto index_empty = testing_util.CreateHypotheticalIndex(table_name, {});
  auto index_b = testing_util.CreateHypotheticalIndex(table_name, {"b"});
  auto index_c = testing_util.CreateHypotheticalIndex(table_name, {"c"});
  auto index_cb = testing_util.CreateHypotheticalIndex(table_name, {"c", "b"});

  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>>
      add_expect_indexes_exhaustive = {index_empty, index_b, index_c, index_bc, index_cb};

  auto add_expect_bitset_exhaustive =
      brain::CompressedIndexConfigUtil::GenerateBitSet(
          comp_idx_config, add_expect_indexes_exhaustive);

  EXPECT_EQ(*add_expect_bitset_exhaustive, add_candidates_exhaustive);
}

TEST_F(CompressedIdxConfigTest, DropCandidatesTest) {
  std::string database_name = DEFAULT_DB_NAME;
  index_selection::TestingIndexSelectionUtil testing_util(database_name);

  // Initialization
  std::set<oid_t> ignore_table_oids;
  brain::CompressedIndexConfigUtil::GetIgnoreTables(database_name,
                                                    ignore_table_oids);
  auto config = testing_util.GetQueryStringsWorkload(
      index_selection::QueryStringsWorkloadType::SingleTableNoop);

  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto &table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
  }

  std::string table_name = table_schemas[0].table_name;

  auto index_ab = testing_util.CreateHypotheticalIndex(table_name, {"a", "b"});
  testing_util.CreateIndex(index_ab);
  auto index_bc = testing_util.CreateHypotheticalIndex(table_name, {"b", "c"});
  testing_util.CreateIndex(index_bc);

  auto comp_idx_config =
      brain::CompressedIndexConfigContainer(database_name, ignore_table_oids);
  LOG_DEBUG("bitset: %s", comp_idx_config.ToString().c_str());
  // Total configuration = total number of permutations: 1 * 3! + 3 * 2! + 3 *
  // 1! + 1 = 16
  EXPECT_EQ(comp_idx_config.GetConfigurationCount(), 16);
  // 2 created + PK index being created by default
  EXPECT_FALSE(
      comp_idx_config.EmptyConfig(GetTableOid(database_name, table_name)));
  EXPECT_EQ(
      comp_idx_config.GetNumIndexes(GetTableOid(database_name, table_name)),
      3);

  std::string query_string = query_strings[0];
  boost::dynamic_bitset<> drop_candidates;
  brain::CompressedIndexConfigUtil::DropCandidates(
      comp_idx_config, query_string, drop_candidates);

  // since b is primary key, we will ignore index {a, b}
  std::vector<std::shared_ptr<brain::HypotheticalIndexObject>>
      drop_expect_indexes = {};

  auto drop_expect_bitset = brain::CompressedIndexConfigUtil::GenerateBitSet(
      comp_idx_config, drop_expect_indexes);

  EXPECT_EQ(*drop_expect_bitset, drop_candidates);

}

}  // namespace test
}  // namespace peloton
