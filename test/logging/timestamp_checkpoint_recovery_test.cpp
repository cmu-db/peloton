//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// new_checkpointing_test.cpp
//
// Identification: test/logging/new_checkpointing_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "common/init.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "logging/timestamp_checkpoint_manager.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Checkpointing Tests
//===--------------------------------------------------------------------===//

class TimestampCheckpointingTests : public PelotonTest {};

std::string DB_NAME = "default_database";

TEST_F(TimestampCheckpointingTests, CheckpointRecoveryTest) {
	PelotonInit::Initialize();

  auto &checkpoint_manager = logging::TimestampCheckpointManager::GetInstance();

  // Do checkpoint recovery
  checkpoint_manager.DoRecovery();

  // Schema check
  //LOG_DEBUG("%s", storage::StorageManager::GetInstance()->GetDatabaseWithOffset(1)->GetInfo().c_str());

  std::string sql1 = "SELECT * FROM checkpoint_table_test";
  std::vector<std::string> expected1 = {"0|1.2|aaa", "1|12.34|bbbbbb", "2|12345.7|ccccccccc", "3|0|xxxx"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql1, expected1, false);

  std::string sql2 = "SELECT * FROM checkpoint_index_test";
  std::vector<std::string> expected2 = {"1|2|3|4", "5|6|7|8", "9|10|11|12"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql2, expected2, false);

  std::string sql3 = "SELECT * FROM checkpoint_constraint_test";
  std::vector<std::string> expected3 = {"1|2|3|4|1|3|4", "5|6|7|8|5|7|8", "9|10|11|12|9|11|12"};
  TestingSQLUtil::ExecuteSQLQueryAndCheckResult(sql3, expected3, false);

  /*
  std::string sql3 = "SELECT * FROM out_of_checkpoint_test";
  ResultType res = TestingSQLUtil::ExecuteSQLQuery(sql3);
  EXPECT_TRUE(res == ResultType::FAILURE);
  */

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  EXPECT_FALSE(catalog::Catalog::GetInstance()->ExistsTableByName(DEFAULT_DB_NAME, "out_of_checkpoint", txn));
  txn_manager.CommitTransaction(txn);

  PelotonInit::Shutdown();
}


}
}
