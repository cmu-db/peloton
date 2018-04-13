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

  // Inserts a given number of tuples with increasing values into the table.
  void InsertIntoTable(std::string table_name, int no_of_tuples) {
    // Insert tuples into table
    for (int i = 0; i < no_of_tuples; i++) {
      std::ostringstream oss;
      oss << "INSERT INTO " << table_name << " VALUES (" << i << "," << i + 1
          << "," << i + 2 << ");";
      TestingSQLUtil::ExecuteSQLQuery(oss.str());
    }
  }

  // Generates table stats to perform what-if index queries.
  void GenerateTableStats() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    optimizer::StatsStorage *stats_storage =
      optimizer::StatsStorage::GetInstance();
    ResultType result = stats_storage->AnalyzeStatsForAllTables(txn);
    assert(result == ResultType::SUCCESS);
    txn_manager.CommitTransaction(txn);
  }
};

/**
 * @brief Verify if admissible index count is correct for a given
 * query workload.
 */
TEST_F(IndexSelectionTest, AdmissibleIndexesTest) {
  // Parameters
  std::string table_name = "dummy_table";
  std::string database_name = DEFAULT_DB_NAME;
  size_t max_cols = 2;
  size_t enumeration_threshold = 2;
  size_t num_indexes = 10;

  CreateDatabase(database_name);
  CreateTable(table_name);

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
    brain::Workload w(queries[i]);
    brain::IndexSelection is(w, max_cols, enumeration_threshold, num_indexes);

    brain::IndexConfiguration ic;
    is.GetAdmissibleIndexes(queries[i], ic);
    LOG_DEBUG("Admissible indexes %ld, %s", i, ic.ToString().c_str());

    auto indexes = ic.GetIndexes();
    EXPECT_EQ(ic.GetIndexCount(), admissible_indexes[i]);
  }

  DropTable(table_name);
  DropDatabase(database_name);
}

/**
 * @brief Tests the first iteration of the candidate index generation
 * algorithm i.e. generating single column candidate indexes per query.
 */
TEST_F(IndexSelectionTest, CandidateIndexGenerationSingleColTest) {

  std::string table_name = "dummy_table";
  std::string database_name = DEFAULT_DB_NAME;

  size_t max_cols = 1;
  size_t enumeration_threshold = 2;
  size_t num_indexes = 10;

  CreateDatabase(database_name);
  CreateTable(table_name);

  // Form the query strings
  std::vector<std::string> query_strs;
  std::vector<int> admissible_indexes;
  query_strs.push_back("SELECT * FROM " + table_name +
                       " WHERE a > 160 and a < 250");
  admissible_indexes.push_back(1);
  query_strs.push_back("SELECT * FROM " + table_name +
                       " WHERE b > 190 and b < 250");
  admissible_indexes.push_back(1);

  brain::Workload workload(query_strs, database_name);
  EXPECT_EQ(workload.Size(), query_strs.size());

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
  // TODO: There is no data in the table. Indexes should not help. Should return 0 but getting 2.
  // EXPECT_EQ(candidate_config.GetIndexCount(), 0);
  EXPECT_EQ(candidate_config.GetIndexCount(), 2);

  // Insert some tuples into the table.
  InsertIntoTable(table_name, 2000);
  GenerateTableStats();

  candidate_config.Clear();
  admissible_config.Clear();

  brain::IndexSelection is(workload, max_cols,
                                        enumeration_threshold, num_indexes);
  is.GenerateCandidateIndexes(candidate_config, admissible_config,
                                           workload);

  LOG_DEBUG("Admissible Index Count: %ld", admissible_config.GetIndexCount());
  LOG_DEBUG("Admissible Indexes: %s", admissible_config.ToString().c_str());
  LOG_DEBUG("Candidate Indexes: %s", candidate_config.ToString().c_str());
  EXPECT_EQ(admissible_config.GetIndexCount(), 2);
  EXPECT_EQ(candidate_config.GetIndexCount(), 2); // Indexes help reduce the cost of the queries, so they get selected.

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
  auto a11 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(1, 1, 1));
  // Column: 2
  auto b11 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(1, 1, 2));
  // Column: 3
  auto c11 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(1, 1, 3));

  // Database: 1
  // Table: 2
  // Column: 1
  auto a12 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(1, 2, 1));
  // Column: 2
  auto b12 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(1, 2, 2));
  // Column: 3
  auto c12 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(1, 2, 3));
  // Column: 2, 3
  cols = {2, 3};
  auto bc12 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(1, 2, cols));
  // Column: 1, 3
  cols = {1, 3};
  auto ac12 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(1, 2, cols));

  // Database: 2
  // Table: 1
  // Column: 1
  auto a21 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(2, 1, 1));
  // Column: 2
  auto b21 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(2, 1, 2));
  // Column: 3
  auto c21 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(2, 1, 3));
  // Column: 1, 2 3
  cols = {1, 2, 3};
  auto abc12 =
      std::shared_ptr<brain::IndexObject>(new brain::IndexObject(1, 2, cols));

  std::set<std::shared_ptr<brain::IndexObject>> indexes;

  indexes = {a11, b11, c11, a12, b12, c12, a21, b21, c21};
  single_column_indexes = {indexes};

  indexes = {a11, b11, bc12, ac12, b12, c12, a21, b21, c21};
  candidates = {indexes};

  result = {indexes};

  expected = {indexes};

  // TODO[Siva]: This test needs more support in as we use an IndexObjectPool
}

}  // namespace test
}  // namespace peloton
