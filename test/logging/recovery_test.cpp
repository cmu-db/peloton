// //===----------------------------------------------------------------------===//
// //
// //                         Peloton
// //
// // recovery_test.cpp
// //
// // Identification: test/logging/recovery_test.cpp
// //
// // Copyright (c) 2015-16, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include <dirent.h>
// #include <sys/mman.h>
// #include <sys/stat.h>
// #include <sys/types.h>

// #include "catalog/catalog.h"
// #include "common/harness.h"
// #include "executor/testing_executor_util.h"
// #include "logging/testing_logging_util.h"

// #include "concurrency/transaction_manager_factory.h"
// #include "executor/logical_tile_factory.h"
// #include "index/index.h"
// #include "logging/log_manager.h"
// #include "logging/loggers/wal_frontend_logger.h"
// #include "logging/logging_util.h"
// #include "storage/data_table.h"
// #include "storage/database.h"
// #include "storage/table_factory.h"
// #include "storage/tile.h"

// #include "executor/mock_executor.h"

// #define DEFAULT_RECOVERY_CID 15

// using ::testing::NotNull;
// using ::testing::Return;
// using ::testing::InSequence;

// namespace peloton {
// namespace test {

// //===--------------------------------------------------------------------===//
// // Recovery Tests
// //===--------------------------------------------------------------------===//

// class RecoveryTests : public PelotonTest {};

// std::vector<storage::Tuple *> BuildLoggingTuples(storage::DataTable *table,
//                                                  int num_rows, bool mutate,
//                                                  bool random) {
//   std::vector<storage::Tuple *> tuples;
//   LOG_INFO("build a vector of %d tuples", num_rows);

//   // Random values
//   std::srand(std::time(nullptr));
//   const catalog::Schema *schema = table->GetSchema();
//   // Ensure that the tile group is as expected.
//   PL_ASSERT(schema->GetColumnCount() == 4);

//   // Insert tuples into tile_group.
//   const bool allocate = true;
//   auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

//   for (int rowid = 0; rowid < num_rows; rowid++) {
//     int populate_value = rowid;
//     if (mutate) populate_value *= 3;

//     storage::Tuple *tuple = new storage::Tuple(schema, allocate);

//     // First column is unique in this case
//     tuple->SetValue(0,
//                     type::ValueFactory::GetIntegerValue(
//                         TestingExecutorUtil::PopulatedValue(populate_value, 0)),
//                     testing_pool);

//     // In case of random, make sure this column has duplicated values
//     tuple->SetValue(
//         1,
//         type::ValueFactory::GetIntegerValue(TestingExecutorUtil::PopulatedValue(
//             random ? std::rand() % (num_rows / 3) : populate_value, 1)),
//         testing_pool);

//     tuple->SetValue(2, type::ValueFactory::GetDecimalValue(
//                            TestingExecutorUtil::PopulatedValue(
//                                random ? std::rand() : populate_value, 2)),
//                     testing_pool);

//     // In case of random, make sure this column has duplicated values
//     auto string_value = type::ValueFactory::GetVarcharValue(
//         std::to_string(TestingExecutorUtil::PopulatedValue(
//             random ? std::rand() % (num_rows / 3) : populate_value, 3)));
//     tuple->SetValue(3, string_value, testing_pool);
//     tuples.push_back(tuple);
//   }
//   return tuples;
// }

// TEST_F(RecoveryTests, RestartTest) {
//   auto catalog = catalog::Catalog::GetInstance();
//   LOG_TRACE("Finish creating catalog");
//   LOG_TRACE("Creating recovery_table");
//   auto recovery_table = TestingExecutorUtil::CreateTable(1024);
//   LOG_TRACE("Finish creating recovery_table");

//   size_t tile_group_size = 5;
//   size_t table_tile_group_count = 3;
//   int num_files = 3;

//   auto mutate = true;
//   auto random = false;
//   cid_t default_commit_id = INVALID_CID;
//   cid_t default_delimiter = INVALID_CID;

//   // XXX: for now hardcode for one logger (suffix 0)
//   std::string dir_name = logging::WriteAheadFrontendLogger::wal_directory_path;

//   storage::Database *db = new storage::Database(DEFAULT_DB_ID);
//   catalog->AddDatabase(db);
//   db->AddTable(recovery_table);

//   int num_rows = tile_group_size * table_tile_group_count;
//   std::vector<std::shared_ptr<storage::Tuple>> tuples =
//       TestingLoggingUtil::BuildTuples(recovery_table, num_rows + 2, mutate,
//                                       random);

//   std::vector<logging::TupleRecord> records =
//       TestingLoggingUtil::BuildTupleRecordsForRestartTest(
//           tuples, tile_group_size, table_tile_group_count, 1, 1);

//   logging::LoggingUtil::RemoveDirectory(dir_name.c_str(), false);

//   auto status = logging::LoggingUtil::CreateDirectory(dir_name.c_str(), 0700);
//   EXPECT_TRUE(status);
//   logging::LogManager::GetInstance().SetLogDirectoryName("./");

//   for (int i = 0; i < num_files; i++) {
//     std::string file_name = dir_name + "/" + std::string("peloton_log_") +
//                             std::to_string(i) + std::string(".log");
//     FILE *fp = fopen(file_name.c_str(), "wb");

//     // now set the first 8 bytes to 0 - this is for the max_log id in this file
//     fwrite((void *)&default_commit_id, sizeof(default_commit_id), 1, fp);

//     // now set the next 8 bytes to 0 - this is for the max delimiter in this
//     // file
//     fwrite((void *)&default_delimiter, sizeof(default_delimiter), 1, fp);

//     // First write a begin record
//     CopySerializeOutput output_buffer_begin;
//     logging::TransactionRecord record_begin(LOGRECORD_TYPE_TRANSACTION_BEGIN,
//                                             i + 2);
//     record_begin.Serialize(output_buffer_begin);

//     fwrite(record_begin.GetMessage(), sizeof(char),
//            record_begin.GetMessageLength(), fp);

//     // Now write 5 insert tuple records into this file
//     for (int j = 0; j < (int)tile_group_size; j++) {
//       int num_record = i * tile_group_size + j;
//       CopySerializeOutput output_buffer;
//       records[num_record].Serialize(output_buffer);

//       fwrite(records[num_record].GetMessage(), sizeof(char),
//              records[num_record].GetMessageLength(), fp);
//     }

//     // Now write 1 extra out of range tuple, only in file 0, which
//     // is present at the second-but-last position of this list
//     if (i == 0) {
//       CopySerializeOutput output_buffer_extra;
//       records[num_files * tile_group_size].Serialize(output_buffer_extra);

//       fwrite(records[num_files * tile_group_size].GetMessage(), sizeof(char),
//              records[num_files * tile_group_size].GetMessageLength(), fp);
//     }

// //     // Now write 1 extra delete tuple, only in the last file, which
// //     // is present at the end of this list
// //     if (i == num_files - 1) {
// //       CopySerializeOutput output_buffer_delete;
// //       records[num_files * tile_group_size + 1].Serialize(output_buffer_delete);

// //       fwrite(records[num_files * tile_group_size + 1].GetMessage(),
// //              sizeof(char),
// //              records[num_files * tile_group_size + 1].GetMessageLength(), fp);
// //     }

// //     // Now write commit
// //     logging::TransactionRecord record_commit(LOGRECORD_TYPE_TRANSACTION_COMMIT,
// //                                              i + 2);

// //     CopySerializeOutput output_buffer_commit;
// //     record_commit.Serialize(output_buffer_commit);

// //     fwrite(record_commit.GetMessage(), sizeof(char),
// //            record_commit.GetMessageLength(), fp);

// //     // Now write delimiter
// //     CopySerializeOutput output_buffer_delim;
// //     logging::TransactionRecord record_delim(LOGRECORD_TYPE_ITERATION_DELIMITER,
// //                                             i + 2);

// //     record_delim.Serialize(output_buffer_delim);

// //     fwrite(record_delim.GetMessage(), sizeof(char),
// //            record_delim.GetMessageLength(), fp);

//     fclose(fp);
//   }

//   LOG_INFO("All files created and written to.");
//   int index_count = recovery_table->GetIndexCount();
//   LOG_INFO("Number of indexes on this table: %d", (int)index_count);

//   for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
//     auto index = recovery_table->GetIndex(index_itr);
//     EXPECT_EQ(index->GetNumberOfTuples(), 0);
//   }

//   logging::WriteAheadFrontendLogger wal_fel;

//   EXPECT_EQ(wal_fel.GetMaxDelimiterForRecovery(), num_files + 1);
//   EXPECT_EQ(wal_fel.GetLogFileCounter(), num_files);

//   EXPECT_EQ(recovery_table->GetTupleCount(), 0);

//   LOG_TRACE("recovery_table tile group count before recovery: %ld",
//             recovery_table->GetTileGroupCount());

//   auto &log_manager = logging::LogManager::GetInstance();
//   log_manager.SetGlobalMaxFlushedIdForRecovery(num_files + 1);

//   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

//   wal_fel.DoRecovery();

//   LOG_TRACE("recovery_table tile group count after recovery: %ld",
//             recovery_table->GetTileGroupCount());

//   EXPECT_EQ(recovery_table->GetTupleCount(),
//             tile_group_size * table_tile_group_count - 1);
//   EXPECT_EQ(wal_fel.GetLogFileCursor(), num_files);

//   txn_manager.SetNextCid(5);
//   wal_fel.RecoverIndex();
//   for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
//     auto index = recovery_table->GetIndex(index_itr);
//     EXPECT_EQ(index->GetNumberOfTuples(),
//               tile_group_size * table_tile_group_count - 1);
//   }

//   // TODO check a few more invariants here
//   wal_fel.CreateNewLogFile(false);

//   EXPECT_EQ(wal_fel.GetLogFileCounter(), num_files + 1);

//   wal_fel.TruncateLog(4);

//   for (int i = 1; i <= 2; i++) {
//     struct stat stat_buf;
//     EXPECT_NE(stat((dir_name + "/" + std::string("peloton_log_") +
//                     std::to_string(i) + std::string(".log")).c_str(),
//                    &stat_buf),
//               0);
//   }

//   wal_fel.CreateNewLogFile(true);

//   EXPECT_EQ(wal_fel.GetLogFileCounter(), num_files + 2);

//   status = logging::LoggingUtil::RemoveDirectory(dir_name.c_str(), false);
//   EXPECT_TRUE(status);

//   auto txn = txn_manager.BeginTransaction();
//   catalog->DropDatabaseWithOid(DEFAULT_DB_ID, txn);
//   txn_manager.CommitTransaction(txn);
// }

// TEST_F(RecoveryTests, BasicInsertTest) {
//   auto recovery_table = TestingExecutorUtil::CreateTable(1024);
//   auto catalog = catalog::Catalog::GetInstance();
//   storage::Database *db = new storage::Database(DEFAULT_DB_ID);
//   catalog->AddDatabase(db);
//   db->AddTable(recovery_table);

//   auto tuples = BuildLoggingTuples(recovery_table, 1, false, false);
//   EXPECT_EQ(recovery_table->GetTupleCount(), 0);
//   EXPECT_EQ(recovery_table->GetTileGroupCount(), 1);
//   EXPECT_EQ(tuples.size(), 1);
//   logging::WriteAheadFrontendLogger fel(true);
//   //  auto bel = logging::WriteAheadBackendLogger::GetInstance();
//   cid_t test_commit_id = 10;

//   type::Value val0 = (tuples[0]->GetValue(0));
//   type::Value val1 = (tuples[0]->GetValue(1));
//   type::Value val2 = (tuples[0]->GetValue(2));
//   type::Value val3 = (tuples[0]->GetValue(3));
//   auto curr_rec = new logging::TupleRecord(
//       LOGRECORD_TYPE_TUPLE_INSERT, test_commit_id, recovery_table->GetOid(),
//       ItemPointer(100, 5), INVALID_ITEMPOINTER, tuples[0], DEFAULT_DB_ID);
//   curr_rec->SetTuple(tuples[0]);
//   fel.InsertTuple(curr_rec);
//   delete curr_rec;

//   auto tg_header = recovery_table->GetTileGroupById(100)->GetHeader();
//   EXPECT_TRUE(tg_header->GetBeginCommitId(5) <= test_commit_id);
//   EXPECT_EQ(tg_header->GetEndCommitId(5), MAX_CID);

//   type::Value rval0 = (recovery_table->GetTileGroupById(100)->GetValue(5, 0));
//   CmpBool cmp0 = (val0.CompareEquals(rval0));
//   EXPECT_TRUE(cmp0 == CmpBool::TRUE);
//   type::Value rval1 = (recovery_table->GetTileGroupById(100)->GetValue(5, 1));
//   CmpBool cmp1 = (val1.CompareEquals(rval1));
//   EXPECT_TRUE(cmp1 == CmpBool::TRUE);
//   type::Value rval2 = (recovery_table->GetTileGroupById(100)->GetValue(5, 2));
//   CmpBool cmp2 = (val2.CompareEquals(rval2));
//   EXPECT_TRUE(cmp2 == CmpBool::TRUE);
//   type::Value rval3 = (recovery_table->GetTileGroupById(100)->GetValue(5, 3));
//   CmpBool cmp3 = (val3.CompareEquals(rval3));
//   EXPECT_TRUE(cmp3 == CmpBool::TRUE);

//   EXPECT_EQ(recovery_table->GetTupleCount(), 1);
//   EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);

//   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//   auto txn = txn_manager.BeginTransaction();
//   catalog->DropDatabaseWithOid(DEFAULT_DB_ID, txn);
//   txn_manager.CommitTransaction(txn);
// }

// TEST_F(RecoveryTests, BasicUpdateTest) {
//   auto catalog = catalog::Catalog::GetInstance();
//   auto recovery_table = TestingExecutorUtil::CreateTable(1024);
//   storage::Database *db = new storage::Database(DEFAULT_DB_ID);
//   catalog->AddDatabase(db);
//   db->AddTable(recovery_table);

//   auto tuples = BuildLoggingTuples(recovery_table, 1, false, false);
//   EXPECT_EQ(recovery_table->GetTupleCount(), 0);
//   EXPECT_EQ(recovery_table->GetTileGroupCount(), 1);
//   EXPECT_EQ(tuples.size(), 1);
//   logging::WriteAheadFrontendLogger fel(true);
//   //  auto bel = logging::WriteAheadBackendLogger::GetInstance();
//   cid_t test_commit_id = 10;

//   type::Value val0 = (tuples[0]->GetValue(0));
//   type::Value val1 = (tuples[0]->GetValue(1));
//   type::Value val2 = (tuples[0]->GetValue(2));
//   type::Value val3 = (tuples[0]->GetValue(3));

//   auto curr_rec = new logging::TupleRecord(
//       LOGRECORD_TYPE_TUPLE_UPDATE, test_commit_id, recovery_table->GetOid(),
//       ItemPointer(100, 5), ItemPointer(100, 4), tuples[0], DEFAULT_DB_ID);
//   curr_rec->SetTuple(tuples[0]);
//   fel.UpdateTuple(curr_rec);
//   delete curr_rec;

//   auto tg_header = recovery_table->GetTileGroupById(100)->GetHeader();
//   EXPECT_TRUE(tg_header->GetBeginCommitId(5) <= test_commit_id);
//   EXPECT_EQ(tg_header->GetEndCommitId(5), MAX_CID);
//   EXPECT_EQ(tg_header->GetEndCommitId(4), test_commit_id);

//   type::Value rval0 = (recovery_table->GetTileGroupById(100)->GetValue(5, 0));
//   CmpBool cmp0 = (val0.CompareEquals(rval0));
//   EXPECT_TRUE(cmp0 == CmpBool::TRUE);
//   type::Value rval1 = (recovery_table->GetTileGroupById(100)->GetValue(5, 1));
//   CmpBool cmp1 = (val1.CompareEquals(rval1));
//   EXPECT_TRUE(cmp1 == CmpBool::TRUE);
//   type::Value rval2 = (recovery_table->GetTileGroupById(100)->GetValue(5, 2));
//   CmpBool cmp2 = (val2.CompareEquals(rval2));
//   EXPECT_TRUE(cmp2 == CmpBool::TRUE);
//   type::Value rval3 = (recovery_table->GetTileGroupById(100)->GetValue(5, 3));
//   CmpBool cmp3 = (val3.CompareEquals(rval3));
//   EXPECT_TRUE(cmp3 == CmpBool::TRUE);

//   EXPECT_EQ(recovery_table->GetTupleCount(), 0);
//   EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);

//   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//   auto txn = txn_manager.BeginTransaction();
//   catalog->DropDatabaseWithOid(DEFAULT_DB_ID, txn);
//   txn_manager.CommitTransaction(txn);
// }

// /* (From Joy) TODO FIX this
// TEST_F(RecoveryTests, BasicDeleteTest) {
//   auto recovery_table = ExecutorTestsUtil::CreateTable(1024);
//   auto &manager = catalog::Manager::GetInstance();
//   storage::Database db(DEFAULT_DB_ID);
//   manager.AddDatabase(&db);
//   db.AddTable(recovery_table);

//   EXPECT_EQ(recovery_table->GetNumberOfTuples(), 0);
//   EXPECT_EQ(recovery_table->GetTileGroupCount(), 1);
//   logging::WriteAheadFrontendLogger fel(true);

//   cid_t test_commit_id = 10;

//   auto curr_rec = new logging::TupleRecord(
//       LOGRECORD_TYPE_TUPLE_UPDATE, test_commit_id, recovery_table->GetOid(),
//       INVALID_ITEMPOINTER, ItemPointer(100, 4), nullptr, DEFAULT_DB_ID);
//   fel.DeleteTuple(curr_rec);

//   delete curr_rec;

//   auto tg_header = recovery_table->GetTileGroupById(100)->GetHeader();
//   EXPECT_EQ(tg_header->GetEndCommitId(4), test_commit_id);

//   //  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 1);
//   EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);
// }*/

// TEST_F(RecoveryTests, OutOfOrderCommitTest) {
//   auto recovery_table = TestingExecutorUtil::CreateTable(1024);
//   auto catalog = catalog::Catalog::GetInstance();
//   storage::Database *db = new storage::Database(DEFAULT_DB_ID);
//   catalog->AddDatabase(db);
//   db->AddTable(recovery_table);

//   auto tuples = BuildLoggingTuples(recovery_table, 1, false, false);
//   EXPECT_EQ(recovery_table->GetTupleCount(), 0);
//   EXPECT_EQ(recovery_table->GetTileGroupCount(), 1);
//   EXPECT_EQ(tuples.size(), 1);
//   logging::WriteAheadFrontendLogger fel(true);
//   //  auto bel = logging::WriteAheadBackendLogger::GetInstance();
//   cid_t test_commit_id = 10;

//   auto curr_rec = new logging::TupleRecord(
//       LOGRECORD_TYPE_TUPLE_UPDATE, test_commit_id + 1, recovery_table->GetOid(),
//       INVALID_ITEMPOINTER, ItemPointer(100, 5), nullptr, DEFAULT_DB_ID);
//   fel.DeleteTuple(curr_rec);
//   delete curr_rec;

//   EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);

//   curr_rec = new logging::TupleRecord(
//       LOGRECORD_TYPE_TUPLE_INSERT, test_commit_id, recovery_table->GetOid(),
//       ItemPointer(100, 5), INVALID_ITEMPOINTER, tuples[0], DEFAULT_DB_ID);

//   curr_rec->SetTuple(tuples[0]);
//   fel.InsertTuple(curr_rec);

//   delete curr_rec;

//   auto tg_header = recovery_table->GetTileGroupById(100)->GetHeader();
//   EXPECT_EQ(tg_header->GetEndCommitId(5), test_commit_id + 1);

//   EXPECT_EQ(recovery_table->GetTupleCount(), 0);
//   EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);
// }

// }  // namespace test
// }  // namespace peloton
