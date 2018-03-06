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
#include "sql/testing_sql_util.h"

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
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE checkpoint_table_test (id INTEGER, value VARCHAR(32));");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (0, 'aaa');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (1, 'bbbbbb');");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (2, 'ccccccccc');");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE checkpoint_index_test (pid INTEGER PRIMARY KEY, value REAL);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_index_test VALUES (10, 1.2);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_index_test VALUES (11, 12.34);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_index_test VALUES (12, 12345.678912345);");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (3, 'xxxx');");
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  // generate table and data that will be out of checkpointing.
  TestingSQLUtil::ExecuteSQLQuery("BEGIN;");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (4, 'out of the checkpoint');");

  // do checkpointing
  checkpoint_manager.StartCheckpointing();

  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO checkpoint_table_test VALUES (5, 'out of the checkpoint');");
  TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE out_of_checkpoint_test (pid INTEGER PRIMARY KEY);");
  TestingSQLUtil::ExecuteSQLQuery("INSERT INTO out_of_checkpoint_test VALUES (1);");
  // TestingSQLUtil::ExecuteSQLQuery("CREATE DATABASE out_of_checkpoint;");

  EXPECT_TRUE(checkpoint_manager.GetStatus());

  sleep(5);
  TestingSQLUtil::ExecuteSQLQuery("COMMIT;");

  checkpoint_manager.StopCheckpointing();
  
  EXPECT_FALSE(checkpoint_manager.GetStatus());


  checkpoint_manager.DoRecovery();

  PelotonInit::Shutdown();
}

}
}
