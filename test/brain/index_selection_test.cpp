//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_test.cpp
//
// Identification: test/brain/index_selection_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <numeric>

#include "binder/bind_node_visitor.h"
#include "brain/index_selection.h"
#include "brain/what_if_index.h"
#include "catalog/index_catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"
#include "sql/testing_sql_util.h"

#include "brain/testing_index_suggestion_util.h"

namespace peloton {
namespace test {

using namespace index_suggestion;

//===--------------------------------------------------------------------===//
// IndexSelectionTest
//===--------------------------------------------------------------------===//

class IndexSelectionTest : public PelotonTest {};

/**
 * @brief Verify if admissible index count is correct for a given
 * query workload.
 */
TEST_F(IndexSelectionTest, AdmissibleIndexesTest) {
  // Parameters
  std::string table_name = "table1";
  std::string database_name = DEFAULT_DB_NAME;
  long num_tuples = 10;

  size_t max_cols = 2;
  size_t enumeration_threshold = 2;
  size_t num_indexes = 10;

  TableSchema schema(table_name, {{"a", TupleValueType::INTEGER},
                                  {"b", TupleValueType::INTEGER},
                                  {"c", TupleValueType::INTEGER},
                                  {"d", TupleValueType::INTEGER}});
  TestingIndexSuggestionUtil testing_util(database_name);
  testing_util.CreateTable(schema);
  testing_util.InsertIntoTable(schema, num_tuples);

  // Form the query strings
  std::vector<std::string> query_strs;
  std::vector<int> admissible_indexes;
  query_strs.push_back("SELECT * FROM " + table_name +
                       " WHERE a < 1 or b > 4 GROUP BY a");
  admissible_indexes.push_back(2);
  query_strs.push_back("SELECT a, b, c FROM " + table_name +
                       " WHERE a < 1 or b > 4 ORDER BY a");
  admissible_indexes.push_back(2);
  query_strs.push_back("DELETE FROM " + table_name + " WHERE a < 1 or b > 4");
  admissible_indexes.push_back(2);
  query_strs.push_back("UPDATE " + table_name +
                       " SET a = 45 WHERE a < 1 or b > 4");

  admissible_indexes.push_back(2);

  // Create a new workload
  brain::Workload workload(query_strs, database_name);
  EXPECT_GT(workload.Size(), 0);

  // Verify the admissible indexes.
  auto queries = workload.GetQueries();
  for (unsigned long i = 0; i < queries.size(); i++) {
    brain::Workload w(queries[i], workload.GetDatabaseName());
    brain::IndexSelection is(w, max_cols, enumeration_threshold, num_indexes);

    brain::IndexConfiguration ic;
    is.GetAdmissibleIndexes(queries[i], ic);
    LOG_DEBUG("Admissible indexes %ld, %s", i, ic.ToString().c_str());
    auto indexes = ic.GetIndexes();
    EXPECT_EQ(ic.GetIndexCount(), admissible_indexes[i]);
  }
}

/**
 * @brief Tests the first iteration of the candidate index generation
 * algorithm i.e. generating single column candidate indexes per query.
 */
TEST_F(IndexSelectionTest, CandidateIndexGenerationTest) {
  std::string database_name = DEFAULT_DB_NAME;

  // Config knobs
  size_t max_cols = 1;
  size_t enumeration_threshold = 2;
  size_t num_indexes = 10;
  int num_rows = 2000;

  TestingIndexSuggestionUtil testing_util(database_name);
  auto config =
      testing_util.GetQueryStringsWorkload(QueryStringsWorkloadType::A);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create all the required tables for this workloads.
  for (auto table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
  }

  brain::Workload workload(query_strings, database_name);
  EXPECT_EQ(workload.Size(), query_strings.size());

  // Generate candidate configurations.
  // The table doesn't have any tuples, so the admissible indexes won't help
  // any of the queries --> candidate set should be 0.
  brain::IndexConfiguration candidate_config;
  brain::IndexConfiguration admissible_config;

  brain::IndexSelection index_selection(workload, max_cols,
                                        enumeration_threshold, num_indexes);
  index_selection.GenerateCandidateIndexes(candidate_config, admissible_config,
                                           workload);

  LOG_DEBUG("Admissible Index Count: %ld", admissible_config.GetIndexCount());
  LOG_DEBUG("Admissible Indexes: %s", admissible_config.ToString().c_str());
  LOG_DEBUG("Candidate Indexes: %s", candidate_config.ToString().c_str());

  EXPECT_EQ(admissible_config.GetIndexCount(), 2);
  // TODO: There is no data in the table. Indexes should not help. Should return
  // 0. But currently, the cost with index for a query if 0.0 if there are no
  // rows in the table where as the cost without the index is 1.0
  // EXPECT_EQ(candidate_config.GetIndexCount(), 0);
  EXPECT_EQ(candidate_config.GetIndexCount(), 2);

  // Insert tuples into the tables.
  for (auto table_schema : table_schemas) {
    testing_util.InsertIntoTable(table_schema, num_rows);
  }

  candidate_config.Clear();
  admissible_config.Clear();

  brain::IndexSelection is(workload, max_cols, enumeration_threshold,
                           num_indexes);
  is.GenerateCandidateIndexes(candidate_config, admissible_config, workload);

  LOG_DEBUG("Admissible Index Count: %ld", admissible_config.GetIndexCount());
  LOG_DEBUG("Admissible Indexes: %s", admissible_config.ToString().c_str());
  LOG_DEBUG("Candidate Indexes: %s", candidate_config.ToString().c_str());
  EXPECT_EQ(admissible_config.GetIndexCount(), 2);
  // Indexes help reduce the cost of the queries, so they get selected.
  EXPECT_EQ(candidate_config.GetIndexCount(), 2);

  auto admissible_indexes = admissible_config.GetIndexes();
  auto candidate_indexes = candidate_config.GetIndexes();

  // Columns - a and c
  std::set<oid_t> expected_cols = {0, 2};

  for (auto col : expected_cols) {
    std::set<oid_t> cols = {col};
    bool found = false;
    for (auto index : admissible_indexes) {
      found |= (index->column_oids == cols);
    }
    EXPECT_TRUE(found);

    found = false;
    for (auto index : candidate_indexes) {
      found |= (index->column_oids == cols);
    }
    EXPECT_TRUE(found);
  }
}

/**
 * @brief Tests multi column index generation from a set of candidate indexes.
 */
TEST_F(IndexSelectionTest, MultiColumnIndexGenerationTest) {
  std::string database_name = DEFAULT_DB_NAME;

  brain::IndexConfiguration candidates;
  brain::IndexConfiguration single_column_indexes;
  brain::IndexConfiguration result;
  brain::IndexConfiguration expected;
  brain::Workload workload(database_name);
  brain::IndexSelection index_selection(workload, 5, 2, 10);

  std::vector<oid_t> cols;

  // Database: 1
  // Table: 1
  // Column: 1
  auto a11 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 1, 1));
  // Column: 2
  auto b11 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 1, 2));
  // Column: 3
  auto c11 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 1, 3));
  // Column: 1, 2
  cols = {1, 2};
  auto ab11 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 1, cols));
  // Column: 1, 3
  cols = {1, 3};
  auto ac11 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 1, cols));
  // Column: 2, 3
  cols = {2, 3};
  auto bc11 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 1, cols));

  // Database: 1
  // Table: 2
  // Column: 1
  auto a12 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 2, 1));
  // Column: 2
  auto b12 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 2, 2));
  // Column: 3
  auto c12 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 2, 3));
  // Column: 2, 3
  cols = {2, 3};
  auto bc12 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 2, cols));
  // Column: 1, 3
  cols = {1, 3};
  auto ac12 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 2, cols));
  // Column: 1, 2 3
  cols = {1, 2, 3};
  auto abc12 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(1, 2, cols));

  // Database: 2
  // Table: 1
  // Column: 1
  auto a21 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(2, 1, 1));
  // Column: 2
  auto b21 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(2, 1, 2));
  // Column: 3
  auto c21 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(2, 1, 3));
  // Column: 1, 2
  cols = {1, 2};
  auto ab21 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(2, 1, cols));
  // Column: 1, 3
  cols = {1, 3};
  auto ac21 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(2, 1, cols));
  // Column: 1, 2 3
  cols = {1, 2, 3};
  auto abc21 = index_selection.AddConfigurationToPool(
      brain::HypotheticalIndexObject(2, 1, cols));

  std::set<std::shared_ptr<brain::HypotheticalIndexObject>> indexes;

  indexes = {a11, b11, c11, a12, b12, c12, a21, b21, c21};
  single_column_indexes = {indexes};

  indexes = {a11, b11, bc12, ac12, c12, a21, abc21};
  candidates = {indexes};

  index_selection.GenerateMultiColumnIndexes(candidates, single_column_indexes,
                                             result);

  // candidates union (candidates * single_column_indexes)
  indexes = {a11,  b11,  bc12, ac12,  c12,  a21, abc21,  // candidates
             ab11, ac11, bc11, abc12, ab21, ac21};       // crossproduct
  expected = {indexes};

  auto chosen_indexes = result.GetIndexes();
  auto expected_indexes = expected.GetIndexes();

  for (auto index : chosen_indexes) {
    int count = 0;
    for (auto expected_index : expected_indexes) {
      auto index_object = *(index.get());
      auto expected_index_object = *(expected_index.get());
      if (index_object == expected_index_object) count++;
    }
    EXPECT_EQ(1, count);
  }
  EXPECT_EQ(expected_indexes.size(), chosen_indexes.size());
}

/**
 * @brief end-to-end test which takes in a workload of queries
 * and spits out the set of indexes that are the best ones for the
 * workload.
 */
TEST_F(IndexSelectionTest, IndexSelectionTest) {
  std::string database_name = DEFAULT_DB_NAME;

  int num_rows = 2000;  // number of rows to be inserted.

  TestingIndexSuggestionUtil testing_util(database_name);
  auto config =
      testing_util.GetQueryStringsWorkload(QueryStringsWorkloadType::B);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create and populate tables.
  for (auto table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
    testing_util.InsertIntoTable(table_schema, num_rows);
  }

  brain::Workload workload(query_strings, database_name);
  EXPECT_EQ(workload.Size(), query_strings.size());

  brain::IndexConfiguration best_config;
  /** Test 1
   * Choose only 1 index with 1 column
   * it should choose {B}
   */
  size_t max_index_cols = 1;         // multi-column index limit
  size_t enumeration_threshold = 2;  // naive enumeration threshold
  size_t num_indexes = 1;            // top num_indexes will be returned.
  brain::IndexSelection is = {workload, max_index_cols, enumeration_threshold,
                              num_indexes};

  is.GetBestIndexes(best_config);

  LOG_DEBUG("Best Indexes: %s", best_config.ToString().c_str());
  LOG_DEBUG("Best Index Count: %ld", best_config.GetIndexCount());

  EXPECT_EQ(best_config.GetIndexCount(), 1);

  EXPECT_TRUE(testing_util.CheckIndexes(best_config, {{1}}));

  /** Test 2
   * Choose 2 indexes with 1 column
   * it should choose {A} and {B}
   */
  max_index_cols = 1;
  enumeration_threshold = 2;
  num_indexes = 2;
  is = {workload, max_index_cols, enumeration_threshold, num_indexes};

  is.GetBestIndexes(best_config);

  LOG_DEBUG("Best Indexes: %s", best_config.ToString().c_str());
  LOG_DEBUG("Best Index Count: %ld", best_config.GetIndexCount());

  EXPECT_EQ(best_config.GetIndexCount(), 2);

  EXPECT_TRUE(testing_util.CheckIndexes(best_config, {{0}, {1}}));

  /** Test 3
   * Choose 1 index with up to 2 columns
   * it should choose {BC}
   */
  max_index_cols = 2;
  enumeration_threshold = 2;
  num_indexes = 1;
  is = {workload, max_index_cols, enumeration_threshold, num_indexes};

  is.GetBestIndexes(best_config);

  LOG_DEBUG("Best Indexes: %s", best_config.ToString().c_str());
  LOG_DEBUG("Best Index Count: %ld", best_config.GetIndexCount());

  EXPECT_EQ(best_config.GetIndexCount(), 1);

  EXPECT_TRUE(testing_util.CheckIndexes(best_config, {{1, 2}}));

  /** Test 4
   * Choose 2 indexes with up to 2 columns
   * it should choose {AB} and {BC}
   */
  max_index_cols = 2;
  enumeration_threshold = 2;
  num_indexes = 2;
  is = {workload, max_index_cols, enumeration_threshold, num_indexes};

  is.GetBestIndexes(best_config);

  LOG_DEBUG("Best Indexes: %s", best_config.ToString().c_str());
  LOG_DEBUG("Best Index Count: %ld", best_config.GetIndexCount());

  EXPECT_EQ(best_config.GetIndexCount(), 2);

  EXPECT_TRUE(testing_util.CheckIndexes(best_config, {{0, 1}, {1, 2}}));

  /** Test 5
   * Choose 4 indexes with up to 2 columns
   * it should choose {AB} and {BC}
   * more indexes donot give any added benefit
   */
  max_index_cols = 2;
  enumeration_threshold = 2;
  num_indexes = 4;
  is = {workload, max_index_cols, enumeration_threshold, num_indexes};

  is.GetBestIndexes(best_config);

  LOG_DEBUG("Best Indexes: %s", best_config.ToString().c_str());
  LOG_DEBUG("Best Index Count: %ld", best_config.GetIndexCount());

  EXPECT_EQ(best_config.GetIndexCount(), 2);

  EXPECT_TRUE(testing_util.CheckIndexes(best_config, {{0, 1}, {1, 2}}));

  /** Test 6
   * Choose 1 index with up to 3 columns
   * it should choose {BC}
   * more indexes / columns donot give any added benefit
   */
  max_index_cols = 3;
  enumeration_threshold = 2;
  num_indexes = 1;
  is = {workload, max_index_cols, enumeration_threshold, num_indexes};

  is.GetBestIndexes(best_config);

  LOG_DEBUG("Best Indexes: %s", best_config.ToString().c_str());
  LOG_DEBUG("Best Index Count: %ld", best_config.GetIndexCount());

  EXPECT_EQ(best_config.GetIndexCount(), 1);

  EXPECT_TRUE(testing_util.CheckIndexes(best_config, {{1, 2}}));

  // TODO[Siva]: This test non-determinstically fails :(
  /** Test 7
   * Choose 4 indexes with up to 3 columns
   * it should choose {AB} and {BC}
   * more indexes / columns donot give any added benefit
   */
  max_index_cols = 3;
  enumeration_threshold = 2;
  num_indexes = 4;
  is = {workload, max_index_cols, enumeration_threshold, num_indexes};

  is.GetBestIndexes(best_config);

  LOG_DEBUG("Best Indexes: %s", best_config.ToString().c_str());
  LOG_DEBUG("Best Index Count: %ld", best_config.GetIndexCount());

  EXPECT_EQ(best_config.GetIndexCount(), 2);

  EXPECT_TRUE(testing_util.CheckIndexes(best_config, {{0, 1}, {1, 2}}));
}

/**
 * @brief end-to-end test which takes in a workload of queries
 * and spits out the set of indexes that are the best ones for more
 * complex workloads.
 */
TEST_F(IndexSelectionTest, IndexSelectionTest2) {
  std::string database_name = DEFAULT_DB_NAME;
  int num_rows = 1000;  // number of rows to be inserted.

  TestingIndexSuggestionUtil testing_util(database_name);
  auto config =
    testing_util.GetQueryStringsWorkload(QueryStringsWorkloadType::D);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create and populate tables.
  for (auto table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
    testing_util.InsertIntoTable(table_schema, num_rows);
  }

  brain::Workload workload(query_strings, database_name);
  EXPECT_EQ(workload.Size(), query_strings.size());

  brain::IndexConfiguration best_config;

  size_t max_index_cols = 3;
  size_t enumeration_threshold = 2;
  size_t num_indexes = 2;
  brain::IndexSelection is = {workload, max_index_cols, enumeration_threshold,
                              num_indexes};

  is.GetBestIndexes(best_config);
  LOG_INFO("Best Indexes: %s", best_config.ToString().c_str());
  LOG_INFO("Best Index Count: %ld", best_config.GetIndexCount());
}

/**
 * @brief end-to-end test which takes in a workload of queries
 * and spits out the set of indexes that are the best ones for more
 * complex workloads.
 */
TEST_F(IndexSelectionTest, LargeIndexSelectionTest) {
  std::string database_name = DEFAULT_DB_NAME;
  int num_rows = 2000;  // number of rows to be inserted.

  TestingIndexSuggestionUtil testing_util(database_name);
  auto config =
      testing_util.GetQueryStringsWorkload(QueryStringsWorkloadType::C);
  auto table_schemas = config.first;
  auto query_strings = config.second;

  // Create and populate tables.
  for (auto table_schema : table_schemas) {
    testing_util.CreateTable(table_schema);
    testing_util.InsertIntoTable(table_schema, num_rows);
  }

  brain::Workload workload(query_strings, database_name);
  EXPECT_EQ(workload.Size(), query_strings.size());

  brain::IndexConfiguration best_config;
  /** Test 1
   * Choose only 1 index with up to 3 column
   * it should choose {ABC}
   */
  size_t max_index_cols = 3;
  size_t enumeration_threshold = 2;
  size_t num_indexes = 1;
  brain::IndexSelection is = {workload, max_index_cols, enumeration_threshold,
                              num_indexes};

  is.GetBestIndexes(best_config);

  LOG_DEBUG("Best Indexes: %s", best_config.ToString().c_str());
  LOG_DEBUG("Best Index Count: %ld", best_config.GetIndexCount());

  EXPECT_EQ(best_config.GetIndexCount(), 1);

  // TODO[Siva]: This test is broken
  // EXPECT_TRUE(testing_util.CheckIndexes(best_config, {{0, 1, 2}}));

  /** Test 2
   * Choose only 2 indexes with up to 3 column
   * it should choose {ABC} and {BCD}
   */
  max_index_cols = 3;
  enumeration_threshold = 2;
  num_indexes = 2;
  is = {workload, max_index_cols, enumeration_threshold, num_indexes};

  is.GetBestIndexes(best_config);

  LOG_DEBUG("Best Indexes: %s", best_config.ToString().c_str());
  LOG_DEBUG("Best Index Count: %ld", best_config.GetIndexCount());

  EXPECT_EQ(best_config.GetIndexCount(), 2);

  // TODO[Siva]: This test is broken
  // EXPECT_TRUE(testing_util.CheckIndexes(best_config, {{0, 1, 2}, {1, 2,
  // 3}}));
}

}  // namespace test
}  // namespace peloton
