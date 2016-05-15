//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logging_test.cpp
//
// Identification: tests/logging/logging_test.cpp
//
// Copyright (c) 2016, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/logging_util.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"

#include "executor/mock_executor.h"
#include "executor/executor_tests_util.h"
#include "logging/logging_tests_util.h"

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

extern LoggingType peloton_logging_mode;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Logging Tests
//===--------------------------------------------------------------------===//

class LoggingTests : public PelotonTest {};

TEST_F(LoggingTests, BasicLoggingTest) {
  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable(1));

  auto &log_manager = logging::LogManager::GetInstance();

  LoggingScheduler scheduler(2, 1, &log_manager, table.get());

  scheduler.Init();
  // The first txn to commit starts with cid 2
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(2);
  scheduler.BackendLogger(0, 0).Insert(2);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Begin(3);

  scheduler.BackendLogger(0, 0).Commit(2);
  scheduler.BackendLogger(0, 1).Insert(3);
  scheduler.BackendLogger(0, 1).Commit(3);
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  scheduler.BackendLogger(0, 0).Done(1);
  scheduler.Run();

  auto results = scheduler.frontend_threads[0].results;
  EXPECT_EQ(3, results[0]);
  scheduler.Cleanup();
}

TEST_F(LoggingTests, AllCommittedTest) {
  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable(1));

  auto &log_manager = logging::LogManager::GetInstance();

  LoggingScheduler scheduler(2, 1, &log_manager, table.get());

  scheduler.Init();
  // Logger 0 is always the front end logger
  // The first txn to commit starts with cid 2
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(2);
  scheduler.BackendLogger(0, 0).Insert(2);
  scheduler.BackendLogger(0, 0).Commit(2);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Begin(3);
  scheduler.BackendLogger(0, 1).Insert(3);
  scheduler.BackendLogger(0, 1).Commit(3);
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  scheduler.BackendLogger(0, 0).Done(1);
  scheduler.BackendLogger(0, 1).Done(1);
  scheduler.Run();

  auto results = scheduler.frontend_threads[0].results;
  EXPECT_EQ(3, results[0]);
  scheduler.Cleanup();
}

TEST_F(LoggingTests, LaggardTest) {
  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable(1));

  auto &log_manager = logging::LogManager::GetInstance();

  LoggingScheduler scheduler(2, 1, &log_manager, table.get());

  scheduler.Init();
  // Logger 0 is always the front end logger
  // The first txn to commit starts with cid 2
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(2);
  scheduler.BackendLogger(0, 0).Insert(2);
  scheduler.BackendLogger(0, 0).Commit(2);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Begin(3);
  scheduler.BackendLogger(0, 1).Insert(3);
  scheduler.BackendLogger(0, 1).Commit(3);
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  // at this point everyone should be updated to 3
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(4);
  scheduler.BackendLogger(0, 0).Insert(4);
  scheduler.BackendLogger(0, 0).Commit(4);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  scheduler.BackendLogger(0, 0).Done(1);
  scheduler.BackendLogger(0, 1).Done(1);

  scheduler.Run();

  auto results = scheduler.frontend_threads[0].results;
  EXPECT_EQ(3, results[0]);
  EXPECT_EQ(3, results[1]);
  scheduler.Cleanup();
}

TEST_F(LoggingTests, FastLoggerTest) {
  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable(1));

  auto &log_manager = logging::LogManager::GetInstance();

  LoggingScheduler scheduler(2, 1, &log_manager, table.get());

  scheduler.Init();
  // Logger 0 is always the front end logger
  // The first txn to commit starts with cid 2
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(2);
  scheduler.BackendLogger(0, 0).Insert(2);
  scheduler.BackendLogger(0, 0).Commit(2);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Begin(3);
  scheduler.BackendLogger(0, 1).Insert(3);
  scheduler.BackendLogger(0, 1).Commit(3);
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  scheduler.BackendLogger(0, 0).Done(1);
  scheduler.BackendLogger(0, 1).Done(1);
  // at this point everyone should be updated to 3
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(4);
  scheduler.BackendLogger(0, 0).Insert(4);
  scheduler.BackendLogger(0, 0).Commit(4);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Insert(5);
  scheduler.BackendLogger(0, 1).Commit(5);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  scheduler.BackendLogger(0, 0).Done(1);
  scheduler.BackendLogger(0, 1).Done(1);

  scheduler.Run();

  auto results = scheduler.frontend_threads[0].results;
  EXPECT_EQ(3, results[0]);
  EXPECT_EQ(3, results[1]);
  scheduler.Cleanup();
}

TEST_F(LoggingTests, BothPreparingTest) {
  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable(1));

  auto &log_manager = logging::LogManager::GetInstance();

  LoggingScheduler scheduler(2, 1, &log_manager, table.get());

  scheduler.Init();
  // Logger 0 is always the front end logger
  // The first txn to commit starts with cid 2
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(2);
  scheduler.BackendLogger(0, 0).Insert(2);
  scheduler.BackendLogger(0, 0).Commit(2);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Begin(3);
  scheduler.BackendLogger(0, 1).Insert(3);
  scheduler.BackendLogger(0, 1).Commit(3);
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  // at this point everyone should be updated to 3
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(4);
  scheduler.BackendLogger(0, 0).Insert(4);
  scheduler.BackendLogger(0, 0).Commit(4);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Begin(5);
  scheduler.BackendLogger(0, 1).Insert(5);
  scheduler.BackendLogger(0, 1).Commit(5);
  // this prepare should still get a may commit of 3
  scheduler.BackendLogger(0, 1).Prepare();

  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  scheduler.BackendLogger(0, 1).Begin(6);
  scheduler.BackendLogger(0, 1).Insert(6);
  scheduler.BackendLogger(0, 1).Commit(6);
  // this call should get a may commit of 4
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  scheduler.BackendLogger(0, 0).Done(1);
  scheduler.BackendLogger(0, 1).Done(1);

  scheduler.Run();

  auto results = scheduler.frontend_threads[0].results;
  EXPECT_EQ(3, results[0]);
  EXPECT_EQ(3, results[1]);
  EXPECT_EQ(4, results[2]);
  scheduler.Cleanup();
}

TEST_F(LoggingTests, TwoRoundTest) {
  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable(1));

  auto &log_manager = logging::LogManager::GetInstance();

  LoggingScheduler scheduler(2, 1, &log_manager, table.get());

  scheduler.Init();
  // Logger 0 is always the front end logger
  // The first txn to commit starts with cid 2
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(2);
  scheduler.BackendLogger(0, 0).Insert(2);
  scheduler.BackendLogger(0, 0).Commit(2);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Begin(3);
  scheduler.BackendLogger(0, 1).Insert(3);
  scheduler.BackendLogger(0, 1).Commit(3);
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  // at this point everyone should be updated to 3
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(4);
  scheduler.BackendLogger(0, 0).Insert(4);
  scheduler.BackendLogger(0, 0).Commit(4);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Begin(5);
  scheduler.BackendLogger(0, 1).Insert(5);
  scheduler.BackendLogger(0, 1).Commit(5);
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  scheduler.BackendLogger(0, 0).Done(1);
  scheduler.BackendLogger(0, 1).Done(1);

  scheduler.Run();

  auto results = scheduler.frontend_threads[0].results;
  EXPECT_EQ(5, results[1]);
  scheduler.Cleanup();
}

TEST_F(LoggingTests, InsertUpdateDeleteTest) {
  std::unique_ptr<storage::DataTable> table(ExecutorTestsUtil::CreateTable(1));

  auto &log_manager = logging::LogManager::GetInstance();

  LoggingScheduler scheduler(2, 1, &log_manager, table.get());

  scheduler.Init();
  // Logger 0 is always the front end logger
  // The first txn to commit starts with cid 2
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(2);
  scheduler.BackendLogger(0, 0).Insert(2);
  scheduler.BackendLogger(0, 0).Commit(2);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Begin(3);
  scheduler.BackendLogger(0, 1).Update(3);
  scheduler.BackendLogger(0, 1).Commit(3);
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  // at this point everyone should be updated to 3
  scheduler.BackendLogger(0, 0).Prepare();
  scheduler.BackendLogger(0, 0).Begin(4);
  scheduler.BackendLogger(0, 0).Delete(4);
  scheduler.BackendLogger(0, 0).Commit(4);
  scheduler.BackendLogger(0, 1).Prepare();
  scheduler.BackendLogger(0, 1).Begin(5);
  scheduler.BackendLogger(0, 1).Delete(5);
  scheduler.BackendLogger(0, 1).Commit(5);
  scheduler.FrontendLogger(0).Collect();
  scheduler.FrontendLogger(0).Flush();
  scheduler.BackendLogger(0, 0).Done(1);
  scheduler.BackendLogger(0, 1).Done(1);

  scheduler.Run();

  auto results = scheduler.frontend_threads[0].results;
  EXPECT_EQ(5, results[1]);
  scheduler.Cleanup();
}

TEST_F(LoggingTests, BasicLogManagerTest) {
  peloton_logging_mode = LOGGING_TYPE_INVALID;
  auto &log_manager = logging::LogManager::GetInstance();
  log_manager.DropFrontendLoggers();
  log_manager.SetLoggingStatus(LOGGING_STATUS_TYPE_INVALID);
  // just start, write a few records and exit
  catalog::Schema *table_schema = new catalog::Schema(
      {ExecutorTestsUtil::GetColumnInfo(0), ExecutorTestsUtil::GetColumnInfo(1),
       ExecutorTestsUtil::GetColumnInfo(2),
       ExecutorTestsUtil::GetColumnInfo(3)});
  std::string table_name("TEST_TABLE");

  // Create table.
  bool own_schema = true;
  bool adapt_table = false;
  storage::DataTable *table = storage::TableFactory::GetDataTable(
      12345, 123456, table_schema, table_name, 1, own_schema, adapt_table);

  storage::Database test_db(12345);
  test_db.AddTable(table);
  catalog::Manager::GetInstance().AddDatabase(&test_db);
  concurrency::TransactionManager &txn_manager =
      concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.BeginTransaction();
  ExecutorTestsUtil::PopulateTable(table, 5, true, false, false);
  txn_manager.CommitTransaction();
  peloton_logging_mode = LOGGING_TYPE_NVM_WAL;

  log_manager.SetSyncCommit(true);
  EXPECT_FALSE(log_manager.ContainsFrontendLogger());
  log_manager.StartStandbyMode();
  log_manager.GetFrontendLogger(0)->SetTestMode(true);
  log_manager.StartRecoveryMode();
  log_manager.WaitForModeTransition(LOGGING_STATUS_TYPE_LOGGING, true);
  EXPECT_TRUE(log_manager.ContainsFrontendLogger());
  log_manager.SetGlobalMaxFlushedCommitId(4);
  concurrency::Transaction test_txn;
  cid_t commit_id = 5;
  log_manager.PrepareLogging();
  log_manager.LogBeginTransaction(commit_id);
  ItemPointer insert_loc(table->GetTileGroup(1)->GetTileGroupId(), 0);
  ItemPointer delete_loc(table->GetTileGroup(2)->GetTileGroupId(), 0);
  ItemPointer update_old(table->GetTileGroup(3)->GetTileGroupId(), 0);
  ItemPointer update_new(table->GetTileGroup(4)->GetTileGroupId(), 0);
  log_manager.LogInsert(commit_id, insert_loc);
  log_manager.LogUpdate(commit_id, update_old, update_new);
  log_manager.LogInsert(commit_id, delete_loc);
  log_manager.LogCommitTransaction(commit_id);
  // since we are doing sync commit we should have reached 5 already
  EXPECT_EQ(5, log_manager.GetPersistentFlushedCommitId());
  log_manager.EndLogging();
}

}  // End test namespace
}  // End peloton namespace
