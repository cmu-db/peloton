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
#include "storage/storage_manager.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Checkpointing Tests
//===--------------------------------------------------------------------===//

class TimestampCheckpointingTests : public PelotonTest {};

std::string DB_NAME = "default_database";


TEST_F(TimestampCheckpointingTests, CheckpointingTest) {
	PelotonInit::Initialize();

  auto &checkpoint_manager = logging::TimestampCheckpointManager::GetInstance();

  // checkpoint_manager.SetCheckpointBaseDirectory("/var/tmp/peloton/checkpoints")

  // generate table and data taken into storage.
  // basic table test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE checkpoint_table_test (id INTEGER, value1 REAL, value2 VARCHAR(32));");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (0, 1.2, 'aaa');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (1, 12.34, 'bbbbbb');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (2, 12345.678912345, 'ccccccccc');");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // primary key and index test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE checkpoint_index_test (pid INTEGER UNIQUE PRIMARY KEY, value1 INTEGER, value2 INTEGER, value3 INTEGER);");
  TestingSQLUtil::ExecuteSQLQuery("CREATE INDEX index_test1 ON checkpoint_index_test USING hash (value1);");
  TestingSQLUtil::ExecuteSQLQuery("CREATE INDEX index_test2 ON checkpoint_index_test USING skiplist (value2, value3);");
  TestingSQLUtil::ExecuteSQLQuery("CREATE UNIQUE INDEX unique_index_test ON checkpoint_index_test (value2);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_index_test VALUES (1, 2, 3, 4);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_index_test VALUES (5, 6, 7, 8);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_index_test VALUES (9, 10, 11, 12);");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // column constraint test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  std::string constraint_test_sql = "CREATE TABLE checkpoint_constraint_test ("
  		"pid1 INTEGER, pid2 INTEGER, "
  		// error "value1 INTEGER UNIQUE"
  		"value1 INTEGER DEFAULT 0 NOT NULL, "
  		"value2 INTEGER CHECK (value2 > 2), "
  		"value3 INTEGER REFERENCES checkpoint_index_test (pid), " // insert doesn't work correctly
  		"value4 INTEGER, value5 INTEGER, "
  		// error "UNIQUE (value4, value5), "
  		// insert doesn't work "FOREIGN KEY (value4, value5) REFERENCES checkpoint_index_test (value2, value3), "
  		"PRIMARY KEY (pid1, pid2));";
  TestingSQLUtil::ExecuteSQLQuery(constraint_test_sql);
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_constraint_test VALUES (1, 2, 3, 4, 1, 3, 4);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_constraint_test VALUES (5, 6, 7, 8, 5, 7, 8);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_constraint_test VALUES (9, 10, 11, 12, 9, 11, 12);");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  sleep(3);

  // insert test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (3, 0.0, 'xxxx');");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // generate table and data that will be out of checkpointing.
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (4, -1.0, 'out of the checkpoint');");

  // do checkpointing
  checkpoint_manager.StartCheckpointing();

  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (5, -2.0, 'out of the checkpoint');");
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE out_of_checkpoint_test (pid INTEGER PRIMARY KEY);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO out_of_checkpoint_test VALUES (1);");
  // TestingSQLUtil::ExecuteSQLQuery("CREATE DATABASE out_of_checkpoint;");

  EXPECT_TRUE(checkpoint_manager.GetStatus());

  sleep(5);
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  checkpoint_manager.StopCheckpointing();
  
  EXPECT_FALSE(checkpoint_manager.GetStatus());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  PelotonInit::Shutdown();
}


TEST_F(TimestampCheckpointingTests, CheckpointRecoveryTest) {
	PelotonInit::Initialize();

  auto &checkpoint_manager = logging::TimestampCheckpointManager::GetInstance();

  // Do checkpoint recovery
  checkpoint_manager.DoRecovery();

  // Schema check
  //LOG_DEBUG("%s", storage::StorageManager::GetInstance()->GetDatabaseWithOffset(1)->GetInfo().c_str());

  std::string sql1 = "SELECT * FROM checkpoint_table_test";
  std::vector<std::string> expected1 = {"0|1.2|'aaa'", "1|12.34|'bbbbbb'", "2|12345.678912345|'ccccccccc'", "3|0.0|'xxxx'"};
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
