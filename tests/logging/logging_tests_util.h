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

#include "harness.h"

#include "backend/logging/log_manager.h"
#include "backend/common/logger.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/loggers/wal_backend_logger.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// LoggingTests Utility
//===--------------------------------------------------------------------===//
enum logging_op_type {
  LOGGING_OP_PREPARE,
  LOGGING_OP_BEGIN,
  LOGGING_OP_INSERT,
  LOGGING_OP_UPDATE,
  LOGGING_OP_DELETE,
  LOGGING_OP_COMMIT,
  LOGGING_OP_ABORT,
  LOGGING_OP_DONE,
  LOGGING_OP_COLLECT,
  LOGGING_OP_FLUSH,
};

class LoggingTestsUtil {
 public:
  static std::vector<logging::TupleRecord> BuildTupleRecords(
      std::vector<std::shared_ptr<storage::Tuple>> &tuples,
      size_t tile_group_size, size_t table_tile_group_count);

  static std::vector<std::shared_ptr<storage::Tuple>> BuildTuples(
      storage::DataTable *table, int num_rows, bool mutate, bool random);
};

// Operation of the logger
struct LoggingOperation {
  logging_op_type op;
  cid_t cid;
  LoggingOperation(logging_op_type op_, cid_t cid_) : op(op_), cid(cid_){};
};

// The schedule for logging execution
struct LoggingSchedule {
  std::vector<LoggingOperation> operations;
  LoggingSchedule() {}
};

// A thread wrapper that runs a backend/frontend logger
class LoggingThread {
 public:
  LoggingThread(LoggingSchedule *sched, logging::LogManager *log_manager_,
                unsigned int thread_id_, storage::DataTable *table_)
      : thread_id(thread_id_),
        schedule(sched),
        log_manager(log_manager_),
        cur_seq(0),
        go(false),
        table(table_) {}

  void RunLoop() {
    if (thread_id != 0) {
      backend_logger = reinterpret_cast<logging::WriteAheadBackendLogger *>(
          log_manager->GetBackendLogger());
    }
    LOG_INFO("Thread %u has %d ops", thread_id,
             (int)schedule->operations.size());
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

  void RunNoWait() {
    if (thread_id != 0) {
      backend_logger = reinterpret_cast<logging::WriteAheadBackendLogger *>(
          log_manager->GetBackendLogger());
    }
    LOG_INFO("Thread %u has %d ops", thread_id,
             (int)schedule->operations.size());
    while (true) {
      ExecuteNext();
      if (cur_seq == (int)schedule->operations.size()) {
        break;
      }
    }
  }

  std::thread Run(bool no_wait = false) {
    if (!no_wait)
      return std::thread(&LoggingThread::RunLoop, this);
    else
      return std::thread(&LoggingThread::RunNoWait, this);
  }

  void ExecuteNext() {
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
        LOG_INFO("Execute Begin %d", (int)cid);
        log_manager->LogBeginTransaction(cid);
        break;
      }
      case LOGGING_OP_INSERT: {
        LOG_INFO("Execute Insert %d", (int)cid);
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
        LOG_INFO("Execute Update %d", (int)cid);
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
        LOG_INFO("Execute Delete %d", (int)cid);
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
        LOG_INFO("Execute Done %d", (int)cid);
        log_manager->DoneLogging();
        break;
      }
      case LOGGING_OP_COMMIT: {
        LOG_INFO("Execute Commit %d", (int)cid);
        std::unique_ptr<logging::LogRecord> record(
            new logging::TransactionRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT,
                                           cid));
        assert(backend_logger);
        backend_logger->Log(record.get());
        break;
      }
      case LOGGING_OP_ABORT: {
        LOG_INFO("Execute Abort %d", (int)cid);
        std::unique_ptr<logging::LogRecord> record(
            new logging::TransactionRecord(LOGRECORD_TYPE_TRANSACTION_ABORT,
                                           cid));
        assert(backend_logger);
        backend_logger->Log(record.get());
        break;
      }
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
    }
  }

  unsigned int thread_id;
  LoggingSchedule *schedule;
  logging::LogManager *log_manager;
  int cur_seq;
  bool go;
  logging::WriteAheadBackendLogger *backend_logger = nullptr;
  logging::WriteAheadFrontendLogger *frontend_logger = nullptr;

  // result of committed cid. used by front end logger only
  std::vector<cid_t> results;
  storage::DataTable *table;
};

// Logging scheduler, to make life easier writing logging test
class LoggingScheduler {
 public:
  LoggingScheduler(size_t num__backend_logger,
                   logging::LogManager *log_manager_,
                   storage::DataTable *table_)
      : log_manager(log_manager_),
        schedules(num__backend_logger + 1),
        table(table_) {}

  void Prepare() {
    schedules[cur_id].operations.emplace_back(LOGGING_OP_PREPARE, INVALID_CID);
    sequence[time++] = cur_id;
  }
  void Begin(cid_t cid) {
    schedules[cur_id].operations.emplace_back(LOGGING_OP_BEGIN, cid);
    sequence[time++] = cur_id;
  }
  void Insert(cid_t cid) {
    schedules[cur_id].operations.emplace_back(LOGGING_OP_INSERT, cid);
    sequence[time++] = cur_id;
  }
  void Delete(cid_t cid) {
    schedules[cur_id].operations.emplace_back(LOGGING_OP_DELETE, cid);
    sequence[time++] = cur_id;
  }
  void Update(cid_t cid) {
    schedules[cur_id].operations.emplace_back(LOGGING_OP_UPDATE, cid);
    sequence[time++] = cur_id;
  }
  void Abort(cid_t cid) {
    schedules[cur_id].operations.emplace_back(LOGGING_OP_ABORT, cid);
    sequence[time++] = cur_id;
  }
  void Commit(cid_t cid) {
    schedules[cur_id].operations.emplace_back(LOGGING_OP_COMMIT, cid);
    sequence[time++] = cur_id;
  }
  void Collect() {
    schedules[cur_id].operations.emplace_back(LOGGING_OP_COLLECT, INVALID_CID);
    sequence[time++] = cur_id;
  }
  void Flush() {
    schedules[cur_id].operations.emplace_back(LOGGING_OP_FLUSH, INVALID_CID);
    sequence[time++] = cur_id;
  }

  // Done is always called after successful flush
  void Done(cid_t cid) {
    schedules[cur_id].operations.emplace_back(LOGGING_OP_DONE, cid);
    sequence[time++] = cur_id;
  }

  void Init() {
    logging::LogManager::Configure(LOGGING_TYPE_DRAM_NVM, true);
    log_manager->SetLoggingStatus(LOGGING_STATUS_TYPE_LOGGING);

    auto frontend_logger =
        reinterpret_cast<logging::WriteAheadFrontendLogger *>(
            log_manager->GetFrontendLogger());
    // Assume txns up to cid = 1 is committed
    frontend_logger->SetMaxFlushedCommitId(1);
    for (int i = 0; i < (int)schedules.size(); i++) {
      log_threads.emplace_back(&schedules[i], log_manager, i, table);
    }
    log_threads[0].frontend_logger = frontend_logger;
  }

  void Run() {
    // Run the txns according to the schedule
    if (!concurrent) {
      for (int i = 0; i < (int)schedules.size(); i++) {
        std::thread t = log_threads[i].Run();
        t.detach();
      }
      for (auto itr = sequence.begin(); itr != sequence.end(); itr++) {
        LOG_INFO("Execute Thread %d", (int)itr->second);
        log_threads[itr->second].go = true;
        while (log_threads[itr->second].go) {
          std::chrono::milliseconds sleep_time(1);
          std::this_thread::sleep_for(sleep_time);
        }
        LOG_INFO("Done Thread %d", (int)itr->second);
      }
    }
    //    else {
    //      // Run the txns concurrently
    //      std::vector<std::thread> threads(schedules.size());
    //      for (int i = 0; i < (int)schedules.size(); i++) {
    //        threads[i] = log_threads[i].Run(true);
    //      }
    //      for (auto &thread : threads) {
    //        thread.join();
    //      }
    //      LOG_INFO("Done concurrent transaction schedule");
    //    }
  }

  LoggingScheduler &Logger(unsigned int id) {
    assert(id < schedules.size());
    cur_id = id;
    return *this;
  }

  // FIXME use smart pointers
  int time = 0;
  logging::LogManager *log_manager;
  std::vector<LoggingSchedule> schedules;
  std::vector<LoggingThread> log_threads;
  std::map<int, int> sequence;
  unsigned int cur_id = 0;
  bool concurrent = false;
  storage::DataTable *table;
};

}  // End test namespace
}  // End peloton namespace
