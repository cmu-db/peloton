//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence_functions_test.cpp
//
// Identification: test/function/sequence_functions_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/sequence_catalog.h"
#include "storage/abstract_table.h"
#include "common/harness.h"
#include "common/exception.h"
#include "executor/executors.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "executor/executor_context.h"
#include "function/sequence_functions.h"
#include "concurrency/transaction_manager_factory.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class SequenceFunctionsTests : public PelotonTest {

 protected:
  void CreateDatabaseHelper() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->Bootstrap();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }

  void CreateSequenceHelper(std::string query,
      concurrency::TransactionContext *txn) {
    auto parser = parser::PostgresParser::GetInstance();

    std::unique_ptr<parser::SQLStatementList> stmt_list(
            parser.BuildParseTree(query).release());
    EXPECT_TRUE(stmt_list->is_valid);
    EXPECT_EQ(StatementType::CREATE, stmt_list->GetStatement(0)->GetType());
    auto create_sequence_stmt =
            static_cast<parser::CreateStatement *>(stmt_list->GetStatement(0));

    create_sequence_stmt->TryBindDatabaseName(DEFAULT_DB_NAME);
    // Create plans
    planner::CreatePlan plan(create_sequence_stmt);

    // plan type
    EXPECT_EQ(CreateType::SEQUENCE, plan.GetCreateType());

    // Execute the create sequence
    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn, {}));
    executor::CreateExecutor createSequenceExecutor(&plan, context.get());
    createSequenceExecutor.Init();
    createSequenceExecutor.Execute();
  }

};
TEST_F(SequenceFunctionsTests, BasicTest) {
  CreateDatabaseHelper();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Create statement
  std::string query = "CREATE SEQUENCE seq;";

  CreateSequenceHelper(query, txn);
  txn_manager.CommitTransaction(txn);
}

TEST_F(SequenceFunctionsTests, FunctionsTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn, {}));

  // Expect exception
  try {
    function::SequenceFunctions::Currval(*(context.get()), "seq");
    EXPECT_EQ(0, 1);
  } catch (const SequenceException &expected) {
    ASSERT_STREQ("currval for sequence \"seq\" is undefined for this session", expected.what());
  }

  // Check nextval & currval functionality
  auto res = function::SequenceFunctions::Nextval(*(context.get()), "seq");
  EXPECT_EQ(1, res);

  res = function::SequenceFunctions::Currval(*(context.get()), "seq");
  EXPECT_EQ(1, res);

  txn_manager.CommitTransaction(txn);
}
}  // namespace test
}  // namespace peloton
