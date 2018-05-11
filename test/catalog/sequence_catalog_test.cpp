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
#include "common/exception.h"
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
    catalog::Catalog::GetInstance()->Bootstrap();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }

  std::shared_ptr<catalog::SequenceCatalogObject> GetSequenceHelper(
      std::string sequence_name, concurrency::TransactionContext *txn) {
    // Check the effect of creation
    oid_t database_oid = catalog::Catalog::GetInstance()
                             ->GetDatabaseWithName(DEFAULT_DB_NAME, txn)
                             ->GetOid();
    std::shared_ptr<catalog::SequenceCatalogObject> new_sequence =
        catalog::Catalog::GetInstance()
                  ->GetSystemCatalogs(database_oid)
                  ->GetSequenceCatalog()
                  ->GetSequence(database_oid, sequence_name, txn);

    return new_sequence;
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
        new executor::ExecutorContext(txn));
    executor::CreateExecutor createSequenceExecutor(&plan, context.get());
    createSequenceExecutor.Init();
    createSequenceExecutor.Execute();
  }
};

TEST_F(SequenceTests, BasicTest) {
  CreateDatabaseHelper();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Create statement
  std::string query =
      "CREATE SEQUENCE seq "
      "INCREMENT BY 2 "
      "MINVALUE 10 MAXVALUE 50 "
      "START 10 CYCLE;";
  std::string name = "seq";

  CreateSequenceHelper(query, txn);
  std::shared_ptr<catalog::SequenceCatalogObject> new_sequence =
      GetSequenceHelper(name, txn);

  EXPECT_EQ(name, new_sequence->seq_name);
  EXPECT_EQ(2, new_sequence->seq_increment);
  EXPECT_EQ(10, new_sequence->seq_min);
  EXPECT_EQ(50, new_sequence->seq_max);
  EXPECT_EQ(10, new_sequence->seq_start);
  EXPECT_EQ(true, new_sequence->seq_cycle);
  EXPECT_EQ(10, new_sequence->GetNextVal());
  EXPECT_EQ(10, new_sequence->GetCurrVal());

  txn_manager.CommitTransaction(txn);
}

TEST_F(SequenceTests, NoDuplicateTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Create statement
  std::string query =
      "CREATE SEQUENCE seq "
      "INCREMENT BY 2 "
      "MINVALUE 10 MAXVALUE 50 "
      "START 10 CYCLE;";
  std::string name = "seq";

  // Expect exception
  try {
    CreateSequenceHelper(query, txn);
    EXPECT_EQ(0, 1);
  } catch (const SequenceException &expected) {
    ASSERT_STREQ("Sequence seq already exists!",
                 expected.what());
  }
  txn_manager.CommitTransaction(txn);
}

TEST_F(SequenceTests, NextValPosIncrementFunctionalityTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::string query =
      "CREATE SEQUENCE seq1 "
      "INCREMENT BY 1 "
      "MINVALUE 10 MAXVALUE 50 "
      "START 10 CYCLE;";
  std::string name = "seq1";

  CreateSequenceHelper(query, txn);
  std::shared_ptr<catalog::SequenceCatalogObject> new_sequence =
      GetSequenceHelper(name, txn);

  int64_t nextVal = new_sequence->GetNextVal();
  EXPECT_EQ(10, nextVal);
  nextVal = new_sequence->GetNextVal();
  EXPECT_EQ(11, nextVal);

  // test cycle
  new_sequence->SetCurrVal(50);
  nextVal = new_sequence->GetNextVal();
  nextVal = new_sequence->GetNextVal();
  EXPECT_EQ(10, nextVal);

  // test no cycle
  new_sequence->SetCycle(false);
  new_sequence->SetCurrVal(50);

  // Expect exception
  try {
    nextVal = new_sequence->GetNextVal();
    EXPECT_EQ(0, 1);
  } catch (const SequenceException &expected) {
    ASSERT_STREQ("nextval: reached maximum value of sequence seq1 (50)", expected.what());
  }
  txn_manager.CommitTransaction(txn);
}

TEST_F(SequenceTests, NextValNegIncrementFunctionalityTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::string query =
      "CREATE SEQUENCE seq2 "
      "INCREMENT BY -1 "
      "MINVALUE 10 MAXVALUE 50 "
      "START 10 CYCLE;";
  std::string name = "seq2";

  CreateSequenceHelper(query, txn);
  std::shared_ptr<catalog::SequenceCatalogObject> new_sequence =
      GetSequenceHelper(name, txn);

  // test cycle
  int64_t nextVal = new_sequence->GetNextVal();
  EXPECT_EQ(10, nextVal);
  nextVal = new_sequence->GetNextVal();
  EXPECT_EQ(50, nextVal);

  new_sequence->SetCurrVal(49);
  nextVal = new_sequence->GetNextVal();
  nextVal = new_sequence->GetNextVal();
  EXPECT_EQ(48, nextVal);

  // test no cycle
  new_sequence->SetCycle(false);
  new_sequence->SetCurrVal(10);

  // Expect exception
  try {
    nextVal = new_sequence->GetNextVal();
    EXPECT_EQ(0, 1);
  } catch (const SequenceException &expected) {
    ASSERT_STREQ("nextval: reached minimum value of sequence seq2 (10)", expected.what());
  }
  txn_manager.CommitTransaction(txn);
}

TEST_F(SequenceTests, InvalidArgumentTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::string query =
    "CREATE SEQUENCE seq3 "
    "INCREMENT BY -1 "
    "MINVALUE 50 MAXVALUE 10 "
    "START 10 CYCLE;";

  try {
    CreateSequenceHelper(query, txn);
    EXPECT_EQ(0, 1);
  } catch (const SequenceException &expected) {
    ASSERT_STREQ("MINVALUE (50) must be less than MAXVALUE (10)", expected.what());
  }

  query =
    "CREATE SEQUENCE seq3 "
    "INCREMENT BY 0 "
    "MINVALUE 10 MAXVALUE 50 "
    "START 10 CYCLE;";

  try {
    CreateSequenceHelper(query, txn);
    EXPECT_EQ(0, 1);
  } catch (const SequenceException &expected) {
    ASSERT_STREQ("INCREMENT must not be zero", expected.what());
  }

  query =
    "CREATE SEQUENCE seq3 "
    "INCREMENT BY 1 "
    "MINVALUE 10 MAXVALUE 50 "
    "START 8 CYCLE;";

  try {
    CreateSequenceHelper(query, txn);
    EXPECT_EQ(0, 1);
  } catch (const SequenceException &expected) {
    ASSERT_STREQ("START value (8) cannot be less than MINVALUE (10)", expected.what());
  }

  query =
    "CREATE SEQUENCE seq3 "
    "INCREMENT BY -1 "
    "MINVALUE 10 MAXVALUE 50 "
    "START 60 CYCLE;";

  try {
    CreateSequenceHelper(query, txn);
    EXPECT_EQ(0, 1);
  } catch (const SequenceException &expected) {
    ASSERT_STREQ("START value (60) cannot be greater than MAXVALUE (50)", expected.what());
  }

  txn_manager.CommitTransaction(txn);
}
}
}
