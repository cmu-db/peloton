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

#include "common/init.h"
#include "common/harness.h"
#include "logging/timestamp_checkpoint_manager.h"
#include "settings/settings_manager.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Checkpointing Tests
//===--------------------------------------------------------------------===//

class TimestampCheckpointingTests : public PelotonTest {};

TEST_F(TimestampCheckpointingTests, CheckpointingTest) {
  PelotonInit::Initialize();

  auto &checkpoint_manager = logging::TimestampCheckpointManager::GetInstance();

  // checkpoint_manager.SetCheckpointBaseDirectory("/var/tmp/peloton/checkpoints")

  // generate table and data taken into storage.
  // basic table test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE checkpoint_table_test (id INTEGER PRIMARY KEY, value1 "
      "REAL, value2 VARCHAR(32));");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (0, 1.2, 'aaa');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (1, 12.34, 'bbbbbb');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (2, 12345.678912345, "
      "'ccccccccc');");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // primary key and index test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE checkpoint_index_test ("
      "upid1 INTEGER UNIQUE PRIMARY KEY, "
      "upid2 INTEGER PRIMARY KEY, "
      "value1 INTEGER, value2 INTEGER, value3 INTEGER);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE INDEX index_test1 ON checkpoint_index_test USING art (value1);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE INDEX index_test2 ON checkpoint_index_test USING skiplist "
      "(value2, value3);");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE UNIQUE INDEX unique_index_test ON checkpoint_index_test "
      "(value2);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_index_test VALUES (1, 2, 3, 4, 5);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_index_test VALUES (6, 7, 8, 9, 10);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_index_test VALUES (11, 12, 13, 14, 15);");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // column constraint test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  std::string constraint_test_sql =
      "CREATE TABLE checkpoint_constraint_test ("
      "pid1 INTEGER, pid2 INTEGER, "
      "value1 INTEGER DEFAULT 0 UNIQUE, "
      "value2 INTEGER NOT NULL CHECK (value2 > 2), "  // check doesn't work
                                                      // correctly
      "value3 INTEGER REFERENCES checkpoint_table_test (id), "
      "value4 INTEGER, value5 INTEGER, "
      "FOREIGN KEY (value4, value5) REFERENCES checkpoint_index_test (upid1, "
      "upid2), "
      // not supported yet "UNIQUE (value4, value5), "
      "PRIMARY KEY (pid1, pid2));";
  TestingSQLUtil::ExecuteSQLQuery(constraint_test_sql);
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_constraint_test VALUES (1, 2, 3, 4, 0, 1, 2);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_constraint_test VALUES (5, 6, 7, 8, 1, 6, 7);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_constraint_test VALUES (9, 10, 11, 12, 2, 11, "
      "12);");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // insert test
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (3, 0.0, 'xxxx');");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // generate table and data that will be out of checkpointing.
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (4, -1.0, 'out of the "
      "checkpoint');");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO checkpoint_table_test VALUES (5, -2.0, 'out of the "
      "checkpoint');");
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE out_of_checkpoint_test (pid INTEGER PRIMARY KEY);");
  TestingSQLUtil::ExecuteSQLQuery(
      "INSERT INTO out_of_checkpoint_test VALUES (1);");
  // TestingSQLUtil::ExecuteSQLQuery("CREATE DATABASE out_of_checkpoint;");

  // do checkpointing
  checkpoint_manager.StartCheckpointing();

  EXPECT_TRUE(checkpoint_manager.GetStatus());

  std::this_thread::sleep_for(std::chrono::seconds(3));
  checkpoint_manager.StopCheckpointing();

  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  EXPECT_FALSE(checkpoint_manager.GetStatus());

  PelotonInit::Shutdown();
}
}
}
