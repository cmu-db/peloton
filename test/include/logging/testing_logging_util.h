//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logging_tests_util.h
//
// Identification: test/include/logging/logging_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"

#include "logging/log_manager.h"
#include "common/logger.h"
#include "logging/loggers/wal_frontend_logger.h"
#include "logging/loggers/wal_backend_logger.h"
#include "logging/records/tuple_record.h"
#include "logging/records/transaction_record.h"
#include "storage/data_table.h"
#include "storage/tile.h"
#include <climits>
#include "../executor/testing_executor_util.h"

#define INVALID_LOGGER_IDX UINT_MAX

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

class TestingLoggingUtil {
 public:
  static std::vector<logging::TupleRecord> BuildTupleRecords(
      std::vector<std::shared_ptr<storage::Tuple>> &tuples,
      size_t tile_group_size, size_t table_tile_group_count);

  static std::vector<logging::TupleRecord> BuildTupleRecordsForRestartTest(
      std::vector<std::shared_ptr<storage::Tuple>> &tuples,
      size_t tile_group_size, size_t table_tile_group_count,
      int out_of_range_tuples, int delete_tuples);

  static std::vector<std::shared_ptr<storage::Tuple>> BuildTuples(
      storage::DataTable *table, int num_rows, bool mutate, bool random);
};

// Operation of the logger
struct LoggingOperation {
  logging_op_type op;
  cid_t cid;
  LoggingOperation(logging_op_type op_, cid_t cid_ = INVALID_CID)
      : op(op_), cid(cid_){};
};

// The schedule for logging execution
struct LoggingSchedule {
  std::vector<LoggingOperation> operations;
  LoggingSchedule() {}
};

class LoggerId {
 public:
  unsigned int front;
  unsigned int back;
  LoggerId(unsigned int front_, unsigned int back_ = INVALID_LOGGER_IDX)
      : front(front_), back(back_) {}

  LoggerId() : front(INVALID_LOGGER_IDX), back(INVALID_LOGGER_IDX) {}
};

// A thread wrapper that runs a frontend logger
class AbstractLoggingThread {
 public:
  AbstractLoggingThread(LoggingSchedule *sched,
                        logging::LogManager *log_manager_,
                        unsigned int frontend_id_, storage::DataTable *table_)
      : frontend_id(frontend_id_),
        schedule(sched),
        log_manager(log_manager_),
        cur_seq(0),
        go(false),
        table(table_) {}

  virtual void RunLoop() = 0;

  void MainLoop();

  std::thread Run() {
    return std::thread(&AbstractLoggingThread::RunLoop, this);
  }

  virtual void ExecuteNext() = 0;

  virtual ~AbstractLoggingThread() {}

  unsigned int frontend_id = INVALID_LOGGER_IDX;
  LoggingSchedule *schedule;
  logging::LogManager *log_manager;
  int cur_seq;
  bool go;
  storage::DataTable *table;
};

class FrontendLoggingThread : public AbstractLoggingThread {
 public:
  FrontendLoggingThread(LoggingSchedule *sched,
                        logging::LogManager *log_manager_,
                        unsigned int frontend_id_, storage::DataTable *table_)
      : AbstractLoggingThread(sched, log_manager_, frontend_id_, table_) {}

  void RunLoop();

  void ExecuteNext();

  ~FrontendLoggingThread() {}

  logging::WriteAheadFrontendLogger *frontend_logger = nullptr;

  // result of committed cid. used by front end logger only
  std::vector<cid_t> results;
};

class BackendLoggingThread : public AbstractLoggingThread {
 public:
  BackendLoggingThread(LoggingSchedule *sched,
                       logging::LogManager *log_manager_,
                       unsigned int frontend_id_, storage::DataTable *table_,
                       unsigned int backend_id_)
      : AbstractLoggingThread(sched, log_manager_, frontend_id_, table_),
        backend_id(backend_id_) {}

  void RunLoop();

  void ExecuteNext();

  ~BackendLoggingThread() {}

  logging::WriteAheadBackendLogger *backend_logger = nullptr;

  unsigned int backend_id = INVALID_LOGGER_IDX;
};

// Logging scheduler, to make life easier writing logging test
class LoggingScheduler {
 public:
  LoggingScheduler(size_t num_backend_logger_per_frontend,
                   unsigned int num_frontend_logger,
                   logging::LogManager *log_manager_,
                   storage::DataTable *table_)
      : log_manager(log_manager_),
        num_frontend_logger(num_frontend_logger),
        num_backend_logger_per_frontend(num_backend_logger_per_frontend),
        frontend_schedules(num_frontend_logger),
        backend_schedules(num_backend_logger_per_frontend *
                          num_frontend_logger),
        table(table_) {}

  void Prepare() {
    backend_schedules[cur_id.back].operations.emplace_back(LOGGING_OP_PREPARE);
    sequence[time++] = cur_id;
  }
  void Begin(cid_t cid) {
    backend_schedules[cur_id.back].operations.emplace_back(LOGGING_OP_BEGIN,
                                                           cid);
    sequence[time++] = cur_id;
  }
  void Insert(cid_t cid) {
    backend_schedules[cur_id.back].operations.emplace_back(LOGGING_OP_INSERT,
                                                           cid);
    sequence[time++] = cur_id;
  }
  void Delete(cid_t cid) {
    backend_schedules[cur_id.back].operations.emplace_back(LOGGING_OP_DELETE,
                                                           cid);
    sequence[time++] = cur_id;
  }
  void Update(cid_t cid) {
    backend_schedules[cur_id.back].operations.emplace_back(LOGGING_OP_UPDATE,
                                                           cid);
    sequence[time++] = cur_id;
  }
  void Abort(cid_t cid) {
    backend_schedules[cur_id.back].operations.emplace_back(LOGGING_OP_ABORT,
                                                           cid);
    sequence[time++] = cur_id;
  }
  void Commit(cid_t cid) {
    backend_schedules[cur_id.back].operations.emplace_back(LOGGING_OP_COMMIT,
                                                           cid);
    sequence[time++] = cur_id;
  }
  void Done(cid_t cid) {
    backend_schedules[cur_id.back].operations.emplace_back(LOGGING_OP_DONE,
                                                           cid);
    sequence[time++] = cur_id;
  }
  void Collect() {
    frontend_schedules[cur_id.front].operations.emplace_back(
        LOGGING_OP_COLLECT);
    sequence[time++] = cur_id;
  }
  void Flush() {
    frontend_schedules[cur_id.front].operations.emplace_back(LOGGING_OP_FLUSH);
    sequence[time++] = cur_id;
  }

  void Init();

  void Cleanup();

  void Run();

  LoggingScheduler &BackendLogger(unsigned int frontend_idx,
                                  unsigned int backend_idx) {
    PELOTON_ASSERT(frontend_idx < frontend_schedules.size());
    PELOTON_ASSERT(backend_idx < num_backend_logger_per_frontend);
    cur_id.front = frontend_idx;
    cur_id.back = GetBackendLoggerId(frontend_idx, backend_idx);
    return *this;
  }

  LoggingScheduler &FrontendLogger(unsigned int frontend_idx) {
    PELOTON_ASSERT(frontend_idx < frontend_schedules.size());
    cur_id.front = frontend_idx;
    cur_id.back = INVALID_LOGGER_IDX;
    return *this;
  }

  int time = 0;
  logging::LogManager *log_manager;

  unsigned int num_frontend_logger = 1;
  unsigned int num_backend_logger_per_frontend = 2;

  // the logging schedules for frontend and backend loggers
  std::vector<LoggingSchedule> frontend_schedules;
  std::vector<LoggingSchedule> backend_schedules;

  // the logging threads for frontend and backend loggers
  std::vector<FrontendLoggingThread> frontend_threads;
  std::vector<BackendLoggingThread> backend_threads;

  // the sequence of operation
  std::map<int, LoggerId> sequence;

  // current id of frontend & backend loggers
  LoggerId cur_id = LoggerId(INVALID_LOGGER_IDX, INVALID_LOGGER_IDX);

  bool concurrent = false;

  storage::DataTable *table;

 private:
  inline unsigned int GetBackendLoggerId(unsigned int frontend_idx,
                                         unsigned int backend_idx) {
    return frontend_idx * num_backend_logger_per_frontend + backend_idx;
  }
};

}  // namespace test
}  // namespace peloton
