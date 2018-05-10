//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lspi_test.cpp
//
// Identification: test/brain/lspi_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>
#include "brain/indextune/lspi/lspi_tuner.h"
#include "brain/indextune/lspi/lstd.h"
#include "brain/indextune/lspi/rlse.h"
#include "brain/util/eigen_util.h"
#include "common/harness.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tensorflow Tests
//===--------------------------------------------------------------------===//

class LSPITests : public PelotonTest {
 private:
  std::string database_name_;

 public:
  LSPITests() {}

  /**
   * @brief Create a new database
   */
  void CreateDatabase(const std::string &db_name) {
    database_name_ = db_name;
    std::string create_db_str = "CREATE DATABASE " + db_name + ";";
    TestingSQLUtil::ExecuteSQLQuery(create_db_str);
  }

  /**
   * @brief Create a new table with schema (a INT, b INT, c INT)
   */
  void CreateTable(const std::string &table_name) {
    std::string create_str =
        "CREATE TABLE " + table_name + "(a INT, b INT, c INT);";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }

  double TimedExecuteQuery(const std::string &query_str) {
    auto start = std::chrono::system_clock::now();

    TestingSQLUtil::ExecuteSQLQuery(query_str);

    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;

    return elapsed_seconds.count();
  }

  void InsertIntoTable(std::string table_name, int no_of_tuples) {
    // Insert tuples into table
    for (int i = 0; i < no_of_tuples; i++) {
      std::ostringstream oss;
      oss << "INSERT INTO " << table_name << " VALUES (" << i << "," << i + 1
          << "," << i + 2 << ");";
      TestingSQLUtil::ExecuteSQLQuery(oss.str());
    }
  }
};

TEST_F(LSPITests, RLSETest) {
  // Attempt to fit y = m*x
  int NUM_SAMPLES = 500;
  int LOG_INTERVAL = 100;
  int m = 3;
  vector_eig data_in = vector_eig::LinSpaced(NUM_SAMPLES, 0, NUM_SAMPLES - 1);
  vector_eig data_out = data_in.array() * m;
  vector_eig loss_vector = vector_eig::Zero(LOG_INTERVAL);
  float prev_loss = std::numeric_limits<float>::max();
  auto model = brain::RLSEModel(1);
  for (int i = 0; i < NUM_SAMPLES; i++) {
    vector_eig feat_vec = data_in.segment(i, 1);
    double value_true = data_out(i);
    double value_pred = model.Predict(feat_vec);
    double loss = fabs(value_pred - value_true);
    loss_vector(i % LOG_INTERVAL) = loss;
    model.Update(feat_vec, value_true);
    if ((i + 1) % LOG_INTERVAL == 0) {
      float curr_loss = loss_vector.array().mean();
      LOG_DEBUG("Loss at %d: %.5f", i + 1, curr_loss);
      EXPECT_LE(curr_loss, prev_loss);
      prev_loss = curr_loss;
    }
  }
}

TEST_F(LSPITests, TuneTest) {
  // Sanity test that all components are running
  // Need more ri
  const std::string database_name = DEFAULT_DB_NAME;
  const std::string table_name = "dummy_table";
  const int num_rows = 200;

  CreateDatabase(database_name);
  std::set<oid_t> ori_table_oids;
  brain::CompressedIndexConfigUtil::GetOriTables(database_name, ori_table_oids);

  CreateTable(table_name);
  InsertIntoTable(table_name, num_rows);

  brain::LSPIIndexTuner index_tuner(database_name, ori_table_oids);

  std::vector<std::string> workload;
  workload.push_back("SELECT * FROM " + table_name +
                     " WHERE a > 160 and a < 250");
  workload.push_back("DELETE FROM " + table_name + " WHERE a < 1 or b > 4");
  workload.push_back("SELECT * FROM " + table_name +
                     " WHERE b > 190 and b < 250");
  workload.push_back("UPDATE " + table_name +
                     " SET a = 45 WHERE a < 1 or b > 4");
  int CATALOG_SYNC_INTERVAL = 2;

  std::vector<double> query_latencies;
  std::vector<std::string> query_strs;
  for (size_t i = 1; i <= workload.size(); i++) {
    auto query = workload[i - 1];
    auto latency = TimedExecuteQuery(query);
    query_strs.push_back(query);
    query_latencies.push_back(latency);
    if (i % CATALOG_SYNC_INTERVAL == 0) {
      index_tuner.Tune(query_strs, query_latencies);
      query_strs.clear();
      query_latencies.clear();
    }
  }
}

}  // namespace test
}  // namespace peloton
