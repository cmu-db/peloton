//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer_test_util.cpp
//
// Identification: test/optimizer/optimizer_test_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"
#include "concurrency/transaction_manager_factory.h"
#include "binder/bind_node_visitor.h"
#include "common/harness.h"
#include "sql/testing_sql_util.h"
#include "parser/postgresparser.h"



namespace peloton {
namespace test {

class OptimizerTestUtil : public PelotonTest {
 public:
  // If overriding, you must call SetUp and Teardown in overriding method to reset the optimizer object
  virtual void SetUp() override {
    // Call parent virtual function first
    PelotonTest::SetUp();

    // Create database
    CreateDatabase();

    // Create Optimizer
    optimizer_.reset(new optimizer::Optimizer());
  }

  virtual void TearDown() override {
    // Destroy test database
    DestroyDatabase();

    // Call parent virtual function
    PelotonTest::TearDown();
  }

  // Creates the following table: table_name(a INT PRIMARY KEY, b DECIMAL, c VARCHAR)
  void CreateTable(const std::string &table_name) {
    std::stringstream ss;
    ss << "CREATE TABLE " << table_name
       << "(a INT PRIMARY KEY, b DECIMAL, c VARCHAR);";
    EXPECT_EQ(TestingSQLUtil::ExecuteSQLQuery(ss.str()), peloton::ResultType::SUCCESS);
  }

  void CreateTable(const std::string &table_name, int num_tuples) {
    CreateTable(table_name);
    InsertData(table_name, num_tuples);
  }

  void AnalyzeTable(const std::string &table_name) {
    std::stringstream ss;
    ss << "ANALYZE " << table_name << ";";
    ResultType result = TestingSQLUtil::ExecuteSQLQuery(ss.str());
    EXPECT_EQ(result, peloton::ResultType::SUCCESS);
    LOG_INFO("Analyzed %s", table_name.c_str());
  }

  void InsertData(const std::string &table_name, int num_tuples) {
    InsertDataHelper(table_name, num_tuples);

    AnalyzeTable(table_name);
  }

  std::shared_ptr<planner::AbstractPlan> GeneratePlan(const std::string query) {
    // Begin Transaction
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    auto plan = GeneratePlanHelper(optimizer_, query, txn);

    txn_manager.CommitTransaction(txn);

    return plan;
  }

  std::shared_ptr<planner::AbstractPlan> GeneratePlan(const std::string query, concurrency::TransactionContext *txn) {
    return GeneratePlanHelper(optimizer_, query, txn);
  }

 private:

  void CreateDatabase() {
    // Create database
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
    txn_manager.CommitTransaction(txn);
  }

  void DestroyDatabase() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(txn, DEFAULT_DB_NAME);
    txn_manager.CommitTransaction(txn);
  }

  void InsertDataHelper(const std::string &table_name, int tuple_count) {
    int batch_size = 1000;
    std::stringstream ss;
    auto count = 0;
    if (tuple_count > batch_size) {
      for (int i = 0; i < tuple_count; i += batch_size) {
        ss.str(std::string());
        ss << "INSERT INTO " << table_name << " VALUES ";
        for (int j = 1; j <= batch_size; j++) {
          count++;
          ss << "(" << count << ", 1.1, 'abcd')";
          if (j < batch_size) {
            ss << ",";
          }
        }
        ss << ";";
        ResultType result = TestingSQLUtil::ExecuteSQLQuery(ss.str());
        EXPECT_EQ(result, peloton::ResultType::SUCCESS);
      }
    } else {
      ss << "INSERT INTO " << table_name << " VALUES ";
      for (int i = 1; i <= tuple_count; i++) {
        ss << "(" << i << ", 1.1, 'abcd')";
        if (i < tuple_count) {
          ss << ",";
        }
        count++;
      }
      ss << ";";
      ResultType result = TestingSQLUtil::ExecuteSQLQuery(ss.str());
      EXPECT_EQ(result, peloton::ResultType::SUCCESS);
    }
    LOG_INFO("Inserted %d rows into %s", count, table_name.c_str());
  }

  std::shared_ptr<planner::AbstractPlan> GeneratePlanHelper(
      std::unique_ptr<optimizer::AbstractOptimizer> &optimizer,
      const std::string query, concurrency::TransactionContext *txn) {
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    auto parsed_stmt = peloton_parser.BuildParseTree(query);

    auto parse_tree = parsed_stmt->GetStatement(0);
    auto bind_node_visitor = binder::BindNodeVisitor(txn, DEFAULT_DB_NAME);
    bind_node_visitor.BindNameToNode(parse_tree);

    return optimizer->BuildPelotonPlanTree(parsed_stmt, txn);
  }

  std::unique_ptr<optimizer::AbstractOptimizer> optimizer_;

};
}
}