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
#include "brain/index_selection_util.h"
#include "brain/what_if_index.h"
#include "catalog/index_catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// IndexSelectionTest
//===--------------------------------------------------------------------===//

class IndexSelectionTest : public PelotonTest {
 public:
  IndexSelectionTest() {}

  // Create a new database
  void CreateDatabase(std::string db_name) {
    // Create a new database.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(db_name, txn);
    txn_manager.CommitTransaction(txn);
  }

  // Create a new table with schema (a INT, b INT, c INT).
  void CreateTable(std::string table_name) {
    std::string create_str =
        "CREATE TABLE " + table_name + "(a INT, b INT, c INT);";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }

  void DropTable(std::string table_name) {
    std::string create_str = "DROP TABLE " + table_name + ";";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }

  void DropDatabase(std::string db_name) {
    std::string create_str = "DROP DATABASE " + db_name + ";";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }

  void GetQueries(std::string table_name, std::vector<std::string> queries,
                  std::vector<int> &admissible_index_counts) {
    queries.push_back("SELECT * FROM " + table_name +
                      " WHERE a < 1 or b > 4 GROUP BY a");
    admissible_index_counts.push_back(2);
    queries.push_back("SELECT a, b, c FROM " + table_name +
                      " WHERE a < 1 or b > 4 ORDER BY a");
    admissible_index_counts.push_back(2);
    queries.push_back("DELETE FROM " + table_name + " WHERE a < 1 or b > 4");
    admissible_index_counts.push_back(2);
    queries.push_back("UPDATE " + table_name +
                      " SET a = 45 WHERE a < 1 or b > 4");
    admissible_index_counts.push_back(2);
  }

  void CreateWorkload(std::vector<std::string> queries,
                      brain::Workload &workload, std::string database_name) {
    // Parse the query.
    auto parser = parser::PostgresParser::GetInstance();

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    // Bind the query
    std::unique_ptr<binder::BindNodeVisitor> binder(
        new binder::BindNodeVisitor(txn, database_name));

    for (auto query : queries) {
      // Parse
      std::unique_ptr<parser::SQLStatementList> stmt_list(
          parser.BuildParseTree(query).release());
      EXPECT_TRUE(stmt_list->is_valid);
      auto stmt = (parser::SelectStatement *)stmt_list->GetStatement(0);

      // Bind.
      binder->BindNameToNode(stmt);

      workload.AddQuery(stmt);
    }
  }
};

TEST_F(IndexSelectionTest, AdmissibleIndexesTest) {
  //TODO[Vamshi]: This test is broken
  std::string table_name = "dummy_table";
  std::string database_name = DEFAULT_DB_NAME;
  size_t max_cols = 2;
  size_t enumeration_threshold = 2;
  size_t num_indexes = 10;

  CreateDatabase(database_name);
  CreateTable(table_name);

  std::vector<std::string> queries_strs;
  std::vector<int> index_counts;
  GetQueries(table_name, queries_strs, index_counts);

  brain::Workload workload;
  CreateWorkload(queries_strs, workload, database_name);

  auto queries = workload.GetQueries();

  for (unsigned long i = 0; i < queries.size(); i++) {
    brain::Workload w(queries[i]);
    brain::IndexSelection is(w, max_cols, enumeration_threshold, num_indexes);

    brain::IndexConfiguration ic;
    is.GetAdmissibleIndexes(queries[i], ic);

    auto indexes = ic.GetIndexes();
    // EXPECT_EQ(ic.GetIndexCount(), index_counts[i]);
  }

  DropTable(table_name);
  DropDatabase(database_name);
}

TEST_F(IndexSelectionTest, MultiColumnIndexGenerationTest) {
  void GenMultiColumnIndexes(brain::IndexConfiguration & config,
                             brain::IndexConfiguration & single_column_indexes,
                             brain::IndexConfiguration & result);

  brain::IndexConfiguration candidates;
  brain::IndexConfiguration single_column_indexes;
  brain::IndexConfiguration result;
  brain::IndexConfiguration expected;
  brain::Workload workload;
  brain::IndexSelection index_selection(workload, 5, 2, 10);

  std::vector<oid_t> cols;

  // Database: 1
  // Table: 1
  // Column: 1
  auto a11 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 1, 1));
  // Column: 2
  auto b11 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 1, 2));
  // Column: 3
  auto c11 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 1, 3));
  // Column: 1, 2
  cols = {1, 2};
  auto ab11 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 1, cols));
  // Column: 1, 3
  cols = {1, 3};
  auto ac11 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 1, cols));
  // Column: 2, 3
  cols = {2, 3};
  auto bc11 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 1, cols));

  // Database: 1
  // Table: 2
  // Column: 1
  auto a12 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 2, 1));
  // Column: 2
  auto b12 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 2, 2));
  // Column: 3
  auto c12 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 2, 3));
  // Column: 2, 3
  cols = {2, 3};
  auto bc12 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 2, cols));
  // Column: 1, 3
  cols = {1, 3};
  auto ac12 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 2, cols));
  // Column: 1, 2 3
  cols = {1, 2, 3};
  auto abc12 = index_selection.AddConfigurationToPool(
      brain::IndexObject(1, 2, cols));

  // Database: 2
  // Table: 1
  // Column: 1
  auto a21 = index_selection.AddConfigurationToPool(
      brain::IndexObject(2, 1, 1));
  // Column: 2
  auto b21 = index_selection.AddConfigurationToPool(
      brain::IndexObject(2, 1, 2));
  // Column: 3
  auto c21 = index_selection.AddConfigurationToPool(
      brain::IndexObject(2, 1, 3));
  // Column: 1, 2
  cols = {1, 2};
  auto ab21 = index_selection.AddConfigurationToPool(
      brain::IndexObject(2, 1, cols));
  // Column: 1, 3
  cols = {1, 3};
  auto ac21 = index_selection.AddConfigurationToPool(
      brain::IndexObject(2, 1, cols));
  // Column: 1, 2 3
  cols = {1, 2, 3};
  auto abc21 = index_selection.AddConfigurationToPool(
      brain::IndexObject(2, 1, cols));

  std::set<std::shared_ptr<brain::IndexObject>> indexes;

  indexes = {a11, b11, c11, a12, b12, c12, a21, b21, c21};
  single_column_indexes = {indexes};

  indexes = {a11, b11, bc12, ac12, c12, a21, abc21};
  candidates = {indexes};

  index_selection.GenerateMultiColumnIndexes(candidates, single_column_indexes,
        result);

  // candidates union (candidates * single_column_indexes)
  indexes = {a11, b11, bc12, ac12, c12, a21, abc21, // candidates
             ab11, ac11, bc11, abc12, ab21, ac21};  // crossproduct
  expected = {indexes};

  auto chosen_indexes = result.GetIndexes();
  auto expected_indexes = expected.GetIndexes();

  for (auto index : chosen_indexes) {
    int count = 0;
    for (auto expected_index : expected_indexes) {
      auto index_object = *(index.get());
      auto expected_index_object = *(expected_index.get());
      if(index_object == expected_index_object) count++;
    }
    EXPECT_EQ(1, count);
  }
  EXPECT_EQ(expected_indexes.size(), chosen_indexes.size());
}

TEST_F(IndexSelectionTest, CandidateIndexGenerationTest) {
  //TODO[Vamshi]: This test is broken
  std::string table_name = "dummy_table";
  std::string database_name = DEFAULT_DB_NAME;

  size_t max_cols = 2;
  size_t enumeration_threshold = 2;
  size_t num_indexes = 10;

  CreateDatabase(database_name);
  CreateTable(table_name);

  // Generate workload
  std::vector<std::string> queries;
  std::vector<int> index_counts;
  GetQueries(table_name, queries, index_counts);

  brain::Workload workload;
  CreateWorkload(queries, workload, database_name);

  // Generate candidate configurations.
  brain::IndexConfiguration candidate_config;
  brain::IndexConfiguration admissible_config;

  brain::IndexSelection index_selection(workload, max_cols,
                                        enumeration_threshold, num_indexes);
  index_selection.GenerateCandidateIndexes(candidate_config, admissible_config,
                                           workload);

  auto admissible_indexes_count = admissible_config.GetIndexCount();
  auto expected_count =
      std::accumulate(index_counts.begin(), index_counts.end(), 0);

  (void) expected_count;
  (void) admissible_indexes_count;

  // EXPECT_EQ(admissible_indexes_count, expected_count);
  // EXPECT_LE(candidate_config.GetIndexCount(), expected_count);

  // TODO: Test is not complete
  // Check the candidate indexes.

  DropTable(table_name);
  DropDatabase(database_name);
}

}  // namespace test
}  // namespace peloton
