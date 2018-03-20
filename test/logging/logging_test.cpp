// //===----------------------------------------------------------------------===//
// //
// //                         Peloton
// //
// // logging_test.cpp
// //
// // Identification: test/logging/logging_test.cpp
// //
// // Copyright (c) 2015-16, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "catalog/catalog.h"
// #include "common/harness.h"
// #include "executor/testing_executor_util.h"
// #include "logging/testing_logging_util.h"

// #include "concurrency/transaction_manager_factory.h"
// #include "executor/logical_tile_factory.h"
// #include "logging/loggers/wal_frontend_logger.h"
// #include "logging/logging_util.h"
// #include "storage/data_table.h"
// #include "storage/database.h"
// #include "storage/table_factory.h"
// #include "storage/tile.h"

// #include "executor/mock_executor.h"

// using ::testing::NotNull;
// using ::testing::Return;
// using ::testing::InSequence;

// extern peloton::LoggingType peloton_logging_mode;

// namespace peloton {
// namespace test {

// //===--------------------------------------------------------------------===//
// // Logging Tests
// //===--------------------------------------------------------------------===//

// class LoggingTests : public PelotonTest {};

// TEST_F(LoggingTests, BasicLoggingTest) {
//   std::unique_ptr<storage::DataTable> table(
//       TestingExecutorUtil::CreateTable(1));

//   auto &log_manager = logging::LogManager::GetInstance();

//   LoggingScheduler scheduler(2, 1, &log_manager, table.get());

//   scheduler.Init();
//   // The first txn to commit starts with cid 2
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(2);
//   scheduler.BackendLogger(0, 0).Insert(2);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Begin(3);

//   scheduler.BackendLogger(0, 0).Commit(2);
//   scheduler.BackendLogger(0, 1).Insert(3);
//   scheduler.BackendLogger(0, 1).Commit(3);
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   scheduler.BackendLogger(0, 0).Done(1);
//   scheduler.Run();

//   auto results = scheduler.frontend_threads[0].results;
//   EXPECT_EQ(3, results[0]);
//   scheduler.Cleanup();
// }

// TEST_F(LoggingTests, AllCommittedTest) {
//   std::unique_ptr<storage::DataTable> table(
//       TestingExecutorUtil::CreateTable(1));

//   auto &log_manager = logging::LogManager::GetInstance();

//   LoggingScheduler scheduler(2, 1, &log_manager, table.get());

//   scheduler.Init();
//   // Logger 0 is always the front end logger
//   // The first txn to commit starts with cid 2
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(2);
//   scheduler.BackendLogger(0, 0).Insert(2);
//   scheduler.BackendLogger(0, 0).Commit(2);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Begin(3);
//   scheduler.BackendLogger(0, 1).Insert(3);
//   scheduler.BackendLogger(0, 1).Commit(3);
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   scheduler.BackendLogger(0, 0).Done(1);
//   scheduler.BackendLogger(0, 1).Done(1);
//   scheduler.Run();

//   auto results = scheduler.frontend_threads[0].results;
//   EXPECT_EQ(3, results[0]);
//   scheduler.Cleanup();
// }

// TEST_F(LoggingTests, LaggardTest) {
//   std::unique_ptr<storage::DataTable> table(
//       TestingExecutorUtil::CreateTable(1));

//   auto &log_manager = logging::LogManager::GetInstance();

//   LoggingScheduler scheduler(2, 1, &log_manager, table.get());

//   scheduler.Init();
//   // Logger 0 is always the front end logger
//   // The first txn to commit starts with cid 2
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(2);
//   scheduler.BackendLogger(0, 0).Insert(2);
//   scheduler.BackendLogger(0, 0).Commit(2);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Begin(3);
//   scheduler.BackendLogger(0, 1).Insert(3);
//   scheduler.BackendLogger(0, 1).Commit(3);
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   // at this point everyone should be updated to 3
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(4);
//   scheduler.BackendLogger(0, 0).Insert(4);
//   scheduler.BackendLogger(0, 0).Commit(4);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   scheduler.BackendLogger(0, 0).Done(1);
//   scheduler.BackendLogger(0, 1).Done(1);

//   scheduler.Run();

//   auto results = scheduler.frontend_threads[0].results;
//   EXPECT_EQ(3, results[0]);
//   EXPECT_EQ(3, results[1]);
//   scheduler.Cleanup();
// }

// TEST_F(LoggingTests, FastLoggerTest) {
//   std::unique_ptr<storage::DataTable> table(
//       TestingExecutorUtil::CreateTable(1));

//   auto &log_manager = logging::LogManager::GetInstance();

//   LoggingScheduler scheduler(2, 1, &log_manager, table.get());

//   scheduler.Init();
//   // Logger 0 is always the front end logger
//   // The first txn to commit starts with cid 2
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(2);
//   scheduler.BackendLogger(0, 0).Insert(2);
//   scheduler.BackendLogger(0, 0).Commit(2);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Begin(3);
//   scheduler.BackendLogger(0, 1).Insert(3);
//   scheduler.BackendLogger(0, 1).Commit(3);
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   scheduler.BackendLogger(0, 0).Done(1);
//   scheduler.BackendLogger(0, 1).Done(1);
//   // at this point everyone should be updated to 3
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(4);
//   scheduler.BackendLogger(0, 0).Insert(4);
//   scheduler.BackendLogger(0, 0).Commit(4);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Insert(5);
//   scheduler.BackendLogger(0, 1).Commit(5);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   scheduler.BackendLogger(0, 0).Done(1);
//   scheduler.BackendLogger(0, 1).Done(1);

//   scheduler.Run();

//   auto results = scheduler.frontend_threads[0].results;
//   EXPECT_EQ(3, results[0]);
//   EXPECT_EQ(3, results[1]);
//   scheduler.Cleanup();
// }

// TEST_F(LoggingTests, BothPreparingTest) {
//   std::unique_ptr<storage::DataTable> table(
//       TestingExecutorUtil::CreateTable(1));

//   auto &log_manager = logging::LogManager::GetInstance();

//   LoggingScheduler scheduler(2, 1, &log_manager, table.get());

//   scheduler.Init();
//   // Logger 0 is always the front end logger
//   // The first txn to commit starts with cid 2
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(2);
//   scheduler.BackendLogger(0, 0).Insert(2);
//   scheduler.BackendLogger(0, 0).Commit(2);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Begin(3);
//   scheduler.BackendLogger(0, 1).Insert(3);
//   scheduler.BackendLogger(0, 1).Commit(3);
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   // at this point everyone should be updated to 3
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(4);
//   scheduler.BackendLogger(0, 0).Insert(4);
//   scheduler.BackendLogger(0, 0).Commit(4);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Begin(5);
//   scheduler.BackendLogger(0, 1).Insert(5);
//   scheduler.BackendLogger(0, 1).Commit(5);
//   // this prepare should still get a may commit of 3
//   scheduler.BackendLogger(0, 1).Prepare();

//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   scheduler.BackendLogger(0, 1).Begin(6);
//   scheduler.BackendLogger(0, 1).Insert(6);
//   scheduler.BackendLogger(0, 1).Commit(6);
//   // this call should get a may commit of 4
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   scheduler.BackendLogger(0, 0).Done(1);
//   scheduler.BackendLogger(0, 1).Done(1);

//   scheduler.Run();

//   auto results = scheduler.frontend_threads[0].results;
//   EXPECT_EQ(3, results[0]);
//   EXPECT_EQ(3, results[1]);
//   EXPECT_EQ(4, results[2]);
//   scheduler.Cleanup();
// }

// TEST_F(LoggingTests, TwoRoundTest) {
//   std::unique_ptr<storage::DataTable> table(
//       TestingExecutorUtil::CreateTable(1));

//   auto &log_manager = logging::LogManager::GetInstance();

//   LoggingScheduler scheduler(2, 1, &log_manager, table.get());

//   scheduler.Init();
//   // Logger 0 is always the front end logger
//   // The first txn to commit starts with cid 2
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(2);
//   scheduler.BackendLogger(0, 0).Insert(2);
//   scheduler.BackendLogger(0, 0).Commit(2);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Begin(3);
//   scheduler.BackendLogger(0, 1).Insert(3);
//   scheduler.BackendLogger(0, 1).Commit(3);
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   // at this point everyone should be updated to 3
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(4);
//   scheduler.BackendLogger(0, 0).Insert(4);
//   scheduler.BackendLogger(0, 0).Commit(4);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Begin(5);
//   scheduler.BackendLogger(0, 1).Insert(5);
//   scheduler.BackendLogger(0, 1).Commit(5);
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   scheduler.BackendLogger(0, 0).Done(1);
//   scheduler.BackendLogger(0, 1).Done(1);

//   scheduler.Run();

//   auto results = scheduler.frontend_threads[0].results;
//   EXPECT_EQ(5, results[1]);
//   scheduler.Cleanup();
// }

// TEST_F(LoggingTests, InsertUpdateDeleteTest) {
//   std::unique_ptr<storage::DataTable> table(
//       TestingExecutorUtil::CreateTable(1));

//   auto &log_manager = logging::LogManager::GetInstance();

//   LoggingScheduler scheduler(2, 1, &log_manager, table.get());

//   scheduler.Init();
//   // Logger 0 is always the front end logger
//   // The first txn to commit starts with cid 2
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(2);
//   scheduler.BackendLogger(0, 0).Insert(2);
//   scheduler.BackendLogger(0, 0).Commit(2);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Begin(3);
//   scheduler.BackendLogger(0, 1).Update(3);
//   scheduler.BackendLogger(0, 1).Commit(3);
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   // at this point everyone should be updated to 3
//   scheduler.BackendLogger(0, 0).Prepare();
//   scheduler.BackendLogger(0, 0).Begin(4);
//   scheduler.BackendLogger(0, 0).Delete(4);
//   scheduler.BackendLogger(0, 0).Commit(4);
//   scheduler.BackendLogger(0, 1).Prepare();
//   scheduler.BackendLogger(0, 1).Begin(5);
//   scheduler.BackendLogger(0, 1).Delete(5);
//   scheduler.BackendLogger(0, 1).Commit(5);
//   scheduler.FrontendLogger(0).Collect();
//   scheduler.FrontendLogger(0).Flush();
//   scheduler.BackendLogger(0, 0).Done(1);
//   scheduler.BackendLogger(0, 1).Done(1);

//   scheduler.Run();

//   auto results = scheduler.frontend_threads[0].results;
//   EXPECT_EQ(5, results[1]);
//   scheduler.Cleanup();
// }

// TEST_F(LoggingTests, BasicLogManagerTest) {
//   peloton_logging_mode = LoggingType::INVALID;
//   auto &log_manager = logging::LogManager::GetInstance();
//   log_manager.DropFrontendLoggers();
//   log_manager.SetLoggingStatus(LoggingStatusType::INVALID);
//   // just start, write a few records and exit
//   catalog::Schema *table_schema =
//       new catalog::Schema({TestingExecutorUtil::GetColumnInfo(0),
//                            TestingExecutorUtil::GetColumnInfo(1),
//                            TestingExecutorUtil::GetColumnInfo(2),
//                            TestingExecutorUtil::GetColumnInfo(3)});
//   std::string table_name("TEST_TABLE");

//   // Create table.
//   bool own_schema = true;
//   bool adapt_table = false;
//   storage::DataTable *table = storage::TableFactory::GetDataTable(
//       12345, 123456, table_schema, table_name, 1, own_schema, adapt_table);

//   storage::Database *test_db = new storage::Database(12345);
//   test_db->AddTable(table);
//   auto catalog = catalog::Catalog::GetInstance();
//   catalog->AddDatabase(test_db);
//   concurrency::TransactionManager &txn_manager =
//       concurrency::TransactionManagerFactory::GetInstance();
//   auto txn = txn_manager.BeginTransaction();
//   TestingExecutorUtil::PopulateTable(table, 5, true, false, false, txn);
//   txn_manager.CommitTransaction(txn);
//   peloton_logging_mode = LoggingType::NVM_WAL;

//   log_manager.SetSyncCommit(true);
//   EXPECT_FALSE(log_manager.ContainsFrontendLogger());
//   log_manager.StartStandbyMode();
//   log_manager.GetFrontendLogger(0)->SetTestMode(true);
//   log_manager.StartRecoveryMode();
//   log_manager.WaitForModeTransition(LoggingStatusType::LOGGING, true);
//   EXPECT_TRUE(log_manager.ContainsFrontendLogger());
//   log_manager.SetGlobalMaxFlushedCommitId(4);
//   concurrency::TransactionContext test_txn;
//   cid_t commit_id = 5;
//   log_manager.PrepareLogging();
//   log_manager.LogBeginTransaction(commit_id);
//   ItemPointer insert_loc(table->GetTileGroup(1)->GetTileGroupId(), 0);
//   ItemPointer delete_loc(table->GetTileGroup(2)->GetTileGroupId(), 0);
//   ItemPointer update_old(table->GetTileGroup(3)->GetTileGroupId(), 0);
//   ItemPointer update_new(table->GetTileGroup(4)->GetTileGroupId(), 0);
//   log_manager.LogInsert(commit_id, insert_loc);
//   log_manager.LogUpdate(commit_id, update_old, update_new);
//   log_manager.LogInsert(commit_id, delete_loc);
//   log_manager.LogCommitTransaction(commit_id);

//   // since we are doing sync commit we should have reached 5 already
//   EXPECT_EQ(commit_id, log_manager.GetPersistentFlushedCommitId());
//   log_manager.EndLogging();
// }

// }  // namespace test
// }  // namespace peloton
// >>>>>>> master
