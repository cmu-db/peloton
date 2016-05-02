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

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <dirent.h>

#include "harness.h"

#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/storage/table_factory.h"
#include "backend/logging/log_manager.h"
#include "backend/index/index.h"

#include "backend/logging/logging_util.h"

#include "logging/logging_tests_util.h"

#include "executor/mock_executor.h"
#include "executor/executor_tests_util.h"

#define DEFAULT_RECOVERY_CID 15

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Recovery Tests
//===--------------------------------------------------------------------===//

class RecoveryTests : public PelotonTest {};

std::vector<storage::Tuple *> BuildLoggingTuples(storage::DataTable *table,
                                                 int num_rows, bool mutate,
                                                 bool random) {
  std::vector<storage::Tuple *> tuples;
  LOG_INFO("build a vector of %d tuples", num_rows);

  // Random values
  std::srand(std::time(nullptr));
  const catalog::Schema *schema = table->GetSchema();
  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  const bool allocate = true;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;
    if (mutate) populate_value *= 3;

    storage::Tuple *tuple = new storage::Tuple(schema, allocate);

    // First column is unique in this case
    tuple->SetValue(0,
                    ValueFactory::GetIntegerValue(
                        ExecutorTestsUtil::PopulatedValue(populate_value, 0)),
                    testing_pool);

    // In case of random, make sure this column has duplicated values
    tuple->SetValue(
        1, ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(
               random ? std::rand() % (num_rows / 3) : populate_value, 1)),
        testing_pool);

    tuple->SetValue(
        2, ValueFactory::GetDoubleValue(ExecutorTestsUtil::PopulatedValue(
               random ? std::rand() : populate_value, 2)),
        testing_pool);

    // In case of random, make sure this column has duplicated values
    Value string_value = ValueFactory::GetStringValue(
        std::to_string(ExecutorTestsUtil::PopulatedValue(
            random ? std::rand() % (num_rows / 3) : populate_value, 3)));
    tuple->SetValue(3, string_value, testing_pool);
    tuples.push_back(tuple);
  }
  return tuples;
}

TEST_F(RecoveryTests, RestartTest) {
  auto recovery_table = ExecutorTestsUtil::CreateTable(1024);
  auto &manager = catalog::Manager::GetInstance();

  size_t tile_group_size = 5;
  size_t table_tile_group_count = 3;
  int num_files = 3;

  auto mutate = true;
  auto random = false;
  cid_t default_commit_id = INVALID_CID;
  cid_t default_delimiter = INVALID_CID;

  std::string dir_name = "pl_log0";
  storage::Database db(DEFAULT_DB_ID);
  manager.AddDatabase(&db);
  db.AddTable(recovery_table);

  int num_rows = tile_group_size * table_tile_group_count;
  std::vector<std::shared_ptr<storage::Tuple>> tuples =
      LoggingTestsUtil::BuildTuples(recovery_table, num_rows + 2, mutate,
                                    random);

  std::vector<logging::TupleRecord> records =
      LoggingTestsUtil::BuildTupleRecordsForRestartTest(
          tuples, tile_group_size, table_tile_group_count, 1, 1);

  logging::LoggingUtil::RemoveDirectory("pl_log0", false);

  auto status = logging::LoggingUtil::CreateDirectory(dir_name.c_str(), 0700);
  EXPECT_EQ(status, true);

  for (int i = 0; i < num_files; i++) {
    std::string file_name = dir_name + "/" + std::string("peloton_log_") +
                            std::to_string(i) + std::string(".log");
    FILE *fp = fopen(file_name.c_str(), "wb");

    // now set the first 8 bytes to 0 - this is for the max_log id in this file
    fwrite((void *)&default_commit_id, sizeof(default_commit_id), 1, fp);

    // now set the next 8 bytes to 0 - this is for the max delimiter in this
    // file
    fwrite((void *)&default_delimiter, sizeof(default_delimiter), 1, fp);

    // First write a begin record
    CopySerializeOutput output_buffer_begin;
    logging::TransactionRecord record_begin(LOGRECORD_TYPE_TRANSACTION_BEGIN,
                                            i + 2);
    record_begin.Serialize(output_buffer_begin);

    fwrite(record_begin.GetMessage(), sizeof(char),
           record_begin.GetMessageLength(), fp);

    // Now write 5 insert tuple records into this file
    for (int j = 0; j < (int)tile_group_size; j++) {
      int num_record = i * tile_group_size + j;
      CopySerializeOutput output_buffer;
      records[num_record].Serialize(output_buffer);

      fwrite(records[num_record].GetMessage(), sizeof(char),
             records[num_record].GetMessageLength(), fp);
    }

    // Now write 1 extra out of range tuple, only in file 0, which
    // is present at the second-but-last position of this list
    if (i == 0) {
      CopySerializeOutput output_buffer_extra;
      records[num_files * tile_group_size].Serialize(output_buffer_extra);

      fwrite(records[num_files * tile_group_size].GetMessage(), sizeof(char),
             records[num_files * tile_group_size].GetMessageLength(), fp);
    }

    // Now write 1 extra delete tuple, only in the last file, which
    // is present at the end of this list
    if (i == num_files - 1) {
      CopySerializeOutput output_buffer_delete;
      records[num_files * tile_group_size + 1].Serialize(output_buffer_delete);

      fwrite(records[num_files * tile_group_size + 1].GetMessage(),
             sizeof(char),
             records[num_files * tile_group_size + 1].GetMessageLength(), fp);
    }

    // Now write commit
    logging::TransactionRecord record_commit(LOGRECORD_TYPE_TRANSACTION_COMMIT,
                                             i + 2);

    CopySerializeOutput output_buffer_commit;
    record_commit.Serialize(output_buffer_commit);

    fwrite(record_commit.GetMessage(), sizeof(char),
           record_commit.GetMessageLength(), fp);

    // Now write delimiter
    CopySerializeOutput output_buffer_delim;
    logging::TransactionRecord record_delim(LOGRECORD_TYPE_ITERATION_DELIMITER,
                                            i + 2);

    record_delim.Serialize(output_buffer_delim);

    fwrite(record_delim.GetMessage(), sizeof(char),
           record_delim.GetMessageLength(), fp);

    fclose(fp);
  }

  LOG_INFO("All files created and written to.");
  int index_count = recovery_table->GetIndexCount();
  LOG_INFO("Number of indexes on this table: %d", (int)index_count);

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = recovery_table->GetIndex(index_itr);
    EXPECT_EQ(index->GetNumberOfTuples(), 0);
  }

  logging::WriteAheadFrontendLogger wal_fel(std::string("pl_log"));

  EXPECT_EQ(wal_fel.GetMaxDelimiterForRecovery(), num_files + 1);
  EXPECT_EQ(wal_fel.GetLogFileCounter(), num_files);

  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 0);

  auto &log_manager = logging::LogManager::GetInstance();
  log_manager.SetGlobalMaxFlushedIdForRecovery(num_files + 1);

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  wal_fel.DoRecovery();

  EXPECT_EQ(recovery_table->GetNumberOfTuples(),
            tile_group_size * table_tile_group_count - 1);
  EXPECT_EQ(wal_fel.GetLogFileCursor(), num_files);

  txn_manager.SetNextCid(5);
  wal_fel.RecoverIndex();
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = recovery_table->GetIndex(index_itr);
    EXPECT_EQ(index->GetNumberOfTuples(),
              tile_group_size * table_tile_group_count - 1);
  }

  // TODO check a few more invariants here
  wal_fel.CreateNewLogFile(false);

  EXPECT_EQ(wal_fel.GetLogFileCounter(), num_files + 1);

  wal_fel.CreateNewLogFile(true);

  EXPECT_EQ(wal_fel.GetLogFileCounter(), num_files + 2);

  status = logging::LoggingUtil::RemoveDirectory("pl_log0", false);
  EXPECT_EQ(status, true);
}

TEST_F(RecoveryTests, BasicInsertTest) {
  auto recovery_table = ExecutorTestsUtil::CreateTable(1024);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database db(DEFAULT_DB_ID);
  manager.AddDatabase(&db);
  db.AddTable(recovery_table);

  auto tuples = BuildLoggingTuples(recovery_table, 1, false, false);
  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 0);
  EXPECT_EQ(recovery_table->GetTileGroupCount(), 1);
  EXPECT_EQ(tuples.size(), 1);
  logging::WriteAheadFrontendLogger fel(true);
  //  auto bel = logging::WriteAheadBackendLogger::GetInstance();
  cid_t test_commit_id = 10;

  Value val0 = tuples[0]->GetValue(0);
  Value val1 = tuples[0]->GetValue(1);
  Value val2 = tuples[0]->GetValue(2);
  Value val3 = tuples[0]->GetValue(3);
  auto curr_rec = new logging::TupleRecord(
      LOGRECORD_TYPE_TUPLE_INSERT, test_commit_id, recovery_table->GetOid(),
      ItemPointer(100, 5), INVALID_ITEMPOINTER, tuples[0], DEFAULT_DB_ID);
  curr_rec->SetTuple(tuples[0]);
  fel.InsertTuple(curr_rec);
  delete curr_rec;

  auto tg_header = recovery_table->GetTileGroupById(100)->GetHeader();
  EXPECT_TRUE(tg_header->GetBeginCommitId(5) <= test_commit_id);
  EXPECT_EQ(tg_header->GetEndCommitId(5), MAX_CID);

  EXPECT_TRUE(
      val0.Compare(recovery_table->GetTileGroupById(100)->GetValue(5, 0)) == 0);
  EXPECT_TRUE(
      val1.Compare(recovery_table->GetTileGroupById(100)->GetValue(5, 1)) == 0);
  EXPECT_TRUE(
      val2.Compare(recovery_table->GetTileGroupById(100)->GetValue(5, 2)) == 0);
  EXPECT_TRUE(
      val3.Compare(recovery_table->GetTileGroupById(100)->GetValue(5, 3)) == 0);

  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 1);
  EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);
}

TEST_F(RecoveryTests, BasicUpdateTest) {
  auto recovery_table = ExecutorTestsUtil::CreateTable(1024);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database db(DEFAULT_DB_ID);
  manager.AddDatabase(&db);
  db.AddTable(recovery_table);

  auto tuples = BuildLoggingTuples(recovery_table, 1, false, false);
  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 0);
  EXPECT_EQ(recovery_table->GetTileGroupCount(), 1);
  EXPECT_EQ(tuples.size(), 1);
  logging::WriteAheadFrontendLogger fel(true);
  //  auto bel = logging::WriteAheadBackendLogger::GetInstance();
  cid_t test_commit_id = 10;

  Value val0 = tuples[0]->GetValue(0);
  Value val1 = tuples[0]->GetValue(1);
  Value val2 = tuples[0]->GetValue(2);
  Value val3 = tuples[0]->GetValue(3);

  auto curr_rec = new logging::TupleRecord(
      LOGRECORD_TYPE_TUPLE_UPDATE, test_commit_id, recovery_table->GetOid(),
      ItemPointer(100, 5), ItemPointer(100, 4), tuples[0], DEFAULT_DB_ID);
  curr_rec->SetTuple(tuples[0]);
  fel.UpdateTuple(curr_rec);
  delete curr_rec;

  auto tg_header = recovery_table->GetTileGroupById(100)->GetHeader();
  EXPECT_TRUE(tg_header->GetBeginCommitId(5) <= test_commit_id);
  EXPECT_EQ(tg_header->GetEndCommitId(5), MAX_CID);
  EXPECT_EQ(tg_header->GetEndCommitId(4), test_commit_id);

  EXPECT_TRUE(
      val0.Compare(recovery_table->GetTileGroupById(100)->GetValue(5, 0)) == 0);
  EXPECT_TRUE(
      val1.Compare(recovery_table->GetTileGroupById(100)->GetValue(5, 1)) == 0);
  EXPECT_TRUE(
      val2.Compare(recovery_table->GetTileGroupById(100)->GetValue(5, 2)) == 0);
  EXPECT_TRUE(
      val3.Compare(recovery_table->GetTileGroupById(100)->GetValue(5, 3)) == 0);

  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 0);
  EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);
}

/* (From Joy) TODO FIX this
TEST_F(RecoveryTests, BasicDeleteTest) {
  auto recovery_table = ExecutorTestsUtil::CreateTable(1024);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database db(DEFAULT_DB_ID);
  manager.AddDatabase(&db);
  db.AddTable(recovery_table);

  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 0);
  EXPECT_EQ(recovery_table->GetTileGroupCount(), 1);
  logging::WriteAheadFrontendLogger fel(true);

  cid_t test_commit_id = 10;

  auto curr_rec = new logging::TupleRecord(
      LOGRECORD_TYPE_TUPLE_UPDATE, test_commit_id, recovery_table->GetOid(),
      INVALID_ITEMPOINTER, ItemPointer(100, 4), nullptr, DEFAULT_DB_ID);
  fel.DeleteTuple(curr_rec);

  delete curr_rec;

  auto tg_header = recovery_table->GetTileGroupById(100)->GetHeader();
  EXPECT_EQ(tg_header->GetEndCommitId(4), test_commit_id);

  //  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 1);
  EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);
}*/

TEST_F(RecoveryTests, OutOfOrderCommitTest) {
  auto recovery_table = ExecutorTestsUtil::CreateTable(1024);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database db(DEFAULT_DB_ID);
  manager.AddDatabase(&db);
  db.AddTable(recovery_table);

  auto tuples = BuildLoggingTuples(recovery_table, 1, false, false);
  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 0);
  EXPECT_EQ(recovery_table->GetTileGroupCount(), 1);
  EXPECT_EQ(tuples.size(), 1);
  logging::WriteAheadFrontendLogger fel(true);
  //  auto bel = logging::WriteAheadBackendLogger::GetInstance();
  cid_t test_commit_id = 10;

  auto curr_rec = new logging::TupleRecord(
      LOGRECORD_TYPE_TUPLE_UPDATE, test_commit_id + 1, recovery_table->GetOid(),
      INVALID_ITEMPOINTER, ItemPointer(100, 5), nullptr, DEFAULT_DB_ID);
  fel.DeleteTuple(curr_rec);
  delete curr_rec;

  EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);

  curr_rec = new logging::TupleRecord(
      LOGRECORD_TYPE_TUPLE_INSERT, test_commit_id, recovery_table->GetOid(),
      ItemPointer(100, 5), INVALID_ITEMPOINTER, tuples[0], DEFAULT_DB_ID);

  curr_rec->SetTuple(tuples[0]);
  fel.InsertTuple(curr_rec);

  delete curr_rec;

  auto tg_header = recovery_table->GetTileGroupById(100)->GetHeader();
  EXPECT_EQ(tg_header->GetEndCommitId(5), test_commit_id + 1);

  EXPECT_EQ(recovery_table->GetNumberOfTuples(), 0);
  EXPECT_EQ(recovery_table->GetTileGroupCount(), 2);
}

}  // End test namespace
}  // End peloton namespace
