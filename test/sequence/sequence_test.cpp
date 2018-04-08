//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence_test.cpp
//
// Identification: test/sequence/sequence_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "catalog/sequence_catalog.h"
#include "storage/abstract_table.h"
#include "common/harness.h"
#include "sequence/sequence.h"
#include "executor/executors.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"
#include "planner/insert_plan.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

class SequenceTests : public PelotonTest {
 protected:
  void CreateDatabaseHelper() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);

    txn_manager.CommitTransaction(txn);
  }

  std::shared_ptr<sequence::Sequence> CreateSequenceHelper(std::string query, std::string sequence_name) {
    // Bootstrap
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto parser = parser::PostgresParser::GetInstance();
    catalog::Catalog::GetInstance()->Bootstrap();

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
    auto txn = txn_manager.BeginTransaction();
    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));
    executor::CreateExecutor createSequenceExecutor(&plan, context.get());
    createSequenceExecutor.Init();
    createSequenceExecutor.Execute();

    // Check the effect of creation
    oid_t database_oid = catalog::Catalog::GetInstance()->GetDatabaseWithName(DEFAULT_DB_NAME, txn)->GetOid();
    std::shared_ptr<sequence::Sequence> new_sequence =
        catalog::SequenceCatalog::GetInstance().GetSequence(database_oid, sequence_name, txn);
    txn_manager.CommitTransaction(txn);

    return new_sequence;
  }
};

TEST_F(SequenceTests, BasicTest) {
  // Create statement
  CreateDatabaseHelper();
  auto parser = parser::PostgresParser::GetInstance();

  std::string query =
      "CREATE SEQUENCE seq "
      "INCREMENT BY 2 "
      "MINVALUE 10 MAXVALUE 50 "
      "START 10 CYCLE;";
  std::string name = "seq";

  std::shared_ptr<sequence::Sequence> new_sequence = CreateSequenceHelper(query, name);
  if(!new_sequence) {
      std::cout << "get nullptr" << std::endl;
  }
  EXPECT_EQ(name, new_sequence->seq_name);
  EXPECT_EQ(2, new_sequence->seq_increment);
  EXPECT_EQ(10, new_sequence->seq_min);
  EXPECT_EQ(50, new_sequence->seq_max);
  EXPECT_EQ(10, new_sequence->seq_start);
  EXPECT_EQ(true, new_sequence->seq_cycle);
  EXPECT_EQ(10, new_sequence->GetCurrVal());
}

}
}
