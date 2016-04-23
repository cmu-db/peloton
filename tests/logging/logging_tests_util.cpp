//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// checkpoint_test.cpp
//
// Identification: tests/logging/checkpoint_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/logging_tests_util.h"

#define DEFAULT_RECOVERY_CID 15

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// LoggingTests Util
//===--------------------------------------------------------------------===//

std::vector<logging::TupleRecord> LoggingTestsUtil::BuildTupleRecords(
    std::vector<std::shared_ptr<storage::Tuple>> &tuples,
    size_t tile_group_size, size_t table_tile_group_count) {
  std::vector<logging::TupleRecord> records;
  for (size_t block = 1; block <= table_tile_group_count; ++block) {
    for (size_t offset = 0; offset < tile_group_size; ++offset) {
      ItemPointer location(block, offset);
      auto &tuple = tuples[(block - 1) * tile_group_size + offset];
      assert(tuple->GetSchema());
      logging::TupleRecord record(
          LOGRECORD_TYPE_WAL_TUPLE_INSERT, INITIAL_TXN_ID, INVALID_OID,
          location, INVALID_ITEMPOINTER, tuple.get(), DEFAULT_DB_ID);
      record.SetTuple(tuple.get());
      records.push_back(record);
    }
  }
  LOG_INFO("Built a vector of %lu tuple WAL insert records", records.size());
  return records;
}

std::vector<std::shared_ptr<storage::Tuple>> LoggingTestsUtil::BuildTuples(
    storage::DataTable *table, int num_rows, bool mutate, bool random) {
  std::vector<std::shared_ptr<storage::Tuple>> tuples;
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

    std::shared_ptr<storage::Tuple> tuple(new storage::Tuple(schema, allocate));

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
    assert(tuple->GetSchema());
    tuples.push_back(std::move(tuple));
  }
  return tuples;
}

// =======================================================================
// Abstract Logging Thread
// =======================================================================
void AbstractLoggingThread::MainLoop() {
  while (true) {
    while (!go) {
      std::chrono::milliseconds sleep_time(1);
      std::this_thread::sleep_for(sleep_time);
    };
    ExecuteNext();
    if (cur_seq == (int)schedule->operations.size()) {
      go = false;
      return;
    }
    go = false;
  }
}

// =======================================================================
// Frontend Logging Thread
// =======================================================================
void FrontendLoggingThread::RunLoop() {
  frontend_logger = reinterpret_cast<logging::WriteAheadFrontendLogger *>(
      log_manager->GetFrontendLogger(frontend_id));

  // Assume txns up to cid = 1 is committed
  frontend_logger->SetMaxFlushedCommitId(1);

  MainLoop();
}

void FrontendLoggingThread::ExecuteNext() {
  // Prepare data for operation
  logging_op_type op = schedule->operations[cur_seq].op;

  cur_seq++;

  // Execute the operation
  switch (op) {
    case LOGGING_OP_COLLECT: {
      LOG_INFO("Execute Collect");
      assert(frontend_logger);
      frontend_logger->CollectLogRecordsFromBackendLoggers();
      break;
    }
    case LOGGING_OP_FLUSH: {
      LOG_INFO("Execute Flush");
      assert(frontend_logger);
      frontend_logger->FlushLogRecords();
      results.push_back(frontend_logger->GetMaxFlushedCommitId());
      break;
    }
    default: {
      LOG_ERROR("Unsupported operation type!");
      assert(false);
      break;
    }
  }
}

void BackendLoggingThread::RunLoop() {
  backend_logger = reinterpret_cast<logging::WriteAheadBackendLogger *>(
      log_manager->GetBackendLogger());

  MainLoop();
}

void BackendLoggingThread::ExecuteNext() {
  // Prepare data for operation
  logging_op_type op = schedule->operations[cur_seq].op;
  cid_t cid = schedule->operations[cur_seq].cid;

  cur_seq++;

  // Execute the operation
  switch (op) {
    case LOGGING_OP_PREPARE: {
      LOG_INFO("Execute Prepare");
      log_manager->PrepareLogging();
      break;
    }
    case LOGGING_OP_BEGIN: {
      LOG_INFO("Execute Begin txn %d", (int)cid);
      log_manager->LogBeginTransaction(cid);
      break;
    }
    case LOGGING_OP_INSERT: {
      LOG_INFO("Execute Insert txn %d", (int)cid);
      auto tuple = LoggingTestsUtil::BuildTuples(table, 1, false, false)[0];
      std::unique_ptr<logging::LogRecord> tuple_record(
          backend_logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_INSERT, cid, 1,
                                         DEFAULT_DB_ID, INVALID_ITEMPOINTER,
                                         INVALID_ITEMPOINTER, tuple.get()));
      backend_logger->Log(tuple_record.get());
      tuple.reset();
      break;
    }
    case LOGGING_OP_UPDATE: {
      LOG_INFO("Execute Update txn %d", (int)cid);
      auto tuple = LoggingTestsUtil::BuildTuples(table, 1, false, false)[0];
      std::unique_ptr<logging::LogRecord> tuple_record(
          backend_logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE, cid, 1,
                                         DEFAULT_DB_ID, INVALID_ITEMPOINTER,
                                         INVALID_ITEMPOINTER, tuple.get()));
      backend_logger->Log(tuple_record.get());
      tuple.reset();
      break;
    }
    case LOGGING_OP_DELETE: {
      LOG_INFO("Execute Delete txn %d", (int)cid);
      auto tuple = LoggingTestsUtil::BuildTuples(table, 1, false, false)[0];
      std::unique_ptr<logging::LogRecord> tuple_record(
          backend_logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_DELETE, cid, 1,
                                         DEFAULT_DB_ID, INVALID_ITEMPOINTER,
                                         INVALID_ITEMPOINTER, tuple.get()));
      backend_logger->Log(tuple_record.get());
      tuple.reset();
      break;
    }
    case LOGGING_OP_DONE: {
      LOG_INFO("Execute Done txn %d", (int)cid);
      log_manager->DoneLogging();
      break;
    }
    case LOGGING_OP_COMMIT: {
      LOG_INFO("Execute Commit txn %d", (int)cid);
      std::unique_ptr<logging::LogRecord> record(new logging::TransactionRecord(
          LOGRECORD_TYPE_TRANSACTION_COMMIT, cid));
      assert(backend_logger);
      backend_logger->Log(record.get());
      break;
    }
    case LOGGING_OP_ABORT: {
      LOG_INFO("Execute Abort txn %d", (int)cid);
      std::unique_ptr<logging::LogRecord> record(new logging::TransactionRecord(
          LOGRECORD_TYPE_TRANSACTION_ABORT, cid));
      assert(backend_logger);
      backend_logger->Log(record.get());
      break;
    }
    default: {
      LOG_ERROR("Unsupported operation type!");
      break;
    }
  }
}

// =========================================================================
// Logging Scheduler
// ==========================================================================
void LoggingScheduler::Run() {
  // Run the txns according to the schedule
  if (!concurrent) {
    for (auto itr = sequence.begin(); itr != sequence.end(); itr++) {
      auto front_id = itr->second.front;
      auto backend_id = itr->second.back;
      assert(front_id != INVALID_LOGGER_IDX);

      // frontend logger's turn
      if (backend_id == INVALID_LOGGER_IDX) {
        LOG_INFO("Execute Frontend Thread %d", (int)front_id);
        frontend_threads[front_id].go = true;
        while (frontend_threads[front_id].go) {
          std::chrono::milliseconds sleep_time(1);
          std::this_thread::sleep_for(sleep_time);
        }
        LOG_INFO("Done Frontend Thread %d", (int)front_id);
      } else {
        // backend logger's turn
        LOG_INFO("Execute Backend Thread (%d, %d)", (int)front_id,
                 (int)backend_id % num_backend_logger_per_frontend);
        backend_threads[backend_id].go = true;
        while (backend_threads[backend_id].go) {
          std::chrono::milliseconds sleep_time(1);
          std::this_thread::sleep_for(sleep_time);
        }
        LOG_INFO("Done Backend Thread (%d, %d)", (int)front_id,
                 (int)backend_id % num_backend_logger_per_frontend);
      }
    }
  }
}

void LoggingScheduler::Init() {
  logging::LogManager::GetInstance().Configure(
      LOGGING_TYPE_NVM_WAL, true, num_frontend_logger, LOGGER_MAPPING_MANUAL);
  log_manager->SetLoggingStatus(LOGGING_STATUS_TYPE_LOGGING);
  log_manager->ResetFrontendLoggers();
  log_manager->InitFrontendLoggers();

  for (unsigned int i = 0; i < num_frontend_logger; i++) {
    frontend_threads.emplace_back(&frontend_schedules[i], log_manager, i,
                                  table);
  }
  for (unsigned int i = 0;
       i < num_frontend_logger * num_backend_logger_per_frontend; i++) {
    backend_threads.emplace_back(&backend_schedules[i], log_manager, i, table,
                                 i % num_backend_logger_per_frontend);
  }

  // Spawn frontend logger threads
  for (int i = 0; i < (int)frontend_schedules.size(); i++) {
    std::thread t = frontend_threads[i].Run();
    t.detach();
  }

  // Spawn backend logger threads
  for (int i = 0; i < (int)backend_schedules.size(); i++) {
    std::thread t = backend_threads[i].Run();
    t.detach();
  }
}

}  // End test namespace
}  // End peloton namespace
