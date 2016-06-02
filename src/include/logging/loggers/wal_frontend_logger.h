//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_frontend_logger.h
//
// Identification: src/logging/loggers/wal_frontend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/frontend_logger.h"
#include "logging/records/tuple_record.h"
#include "logging/log_file.h"
#include "executor/executors.h"

#include <dirent.h>
#include <vector>
#include <set>
#include <chrono>

extern int peloton_flush_frequency_micros;

namespace peloton {

class VarlenPool;

namespace concurrency {
class Transaction;
}

namespace logging {

typedef std::chrono::high_resolution_clock Clock;

typedef std::chrono::microseconds Micros;

typedef std::chrono::time_point<Clock> TimePoint;
//===--------------------------------------------------------------------===//
// Write Ahead Frontend Logger
//===--------------------------------------------------------------------===//

class WriteAheadFrontendLogger : public FrontendLogger {
 public:
  WriteAheadFrontendLogger(void);

  WriteAheadFrontendLogger(bool for_testing);

  WriteAheadFrontendLogger(std::string log_dir);

  ~WriteAheadFrontendLogger(void);

  void FlushLogRecords(void);

  //===--------------------------------------------------------------------===//
  // Recovery
  //===--------------------------------------------------------------------===//

  void DoRecovery(void);

  void RecoverIndex();

  void StartTransactionRecovery(cid_t commit_id);

  void CommitTransactionRecovery(cid_t commit_id);

  void InsertTuple(TupleRecord *recovery_txn);

  void DeleteTuple(TupleRecord *recovery_txn);

  void UpdateTuple(TupleRecord *recovery_txn);

  void AbortActiveTransactions();

  void InitLogFilesList();

  void CreateNewLogFile(bool);

  bool FileSwitchCondIsTrue();

  void OpenNextLogFile();

  LogRecordType GetNextLogRecordTypeForRecovery();

  void TruncateLog(cid_t);

  void InitLogDirectory();

  std::string GetFileNameFromVersion(int);

  std::pair<cid_t, cid_t> ExtractMaxLogIdAndMaxDelimFromLogFileRecords(FILE *);

  void SetLoggerID(int);

  void UpdateMaxDelimiterForRecovery();

  int GetLogFileCursor() { return log_file_cursor_; }

  int GetLogFileCounter() { return log_file_counter_; }

  void InitSelf();

  static constexpr auto wal_directory_path = "wal_log";

 private:
  std::string GetLogFileName(void);

  bool RecoverTableIndexHelper(storage::DataTable *target_table,
                               cid_t start_cid);

  void InsertIndexEntry(storage::Tuple *tuple, storage::DataTable *table,
                        ItemPointer target_location);

  //===--------------------------------------------------------------------===//
  // Member Variables
  //===--------------------------------------------------------------------===//

  // File pointer and descriptor
  FileHandle cur_file_handle;

  // Txn table during recovery
  std::map<txn_id_t, std::vector<TupleRecord *>> recovery_txn_table;

  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  oid_t max_oid = 0;

  cid_t max_cid = 0;

  // pool for allocating non-inlined values
  VarlenPool *recovery_pool;

  // abj1 adding code here!
  std::vector<LogFile *> log_files_;

  int log_file_counter_;

  int log_file_cursor_;

  // for recovery from in memory buffer instead of file.
  char *input_log_buffer;

  std::string peloton_log_directory;

  std::string LOG_FILE_PREFIX = "peloton_log_";

  std::string LOG_FILE_SUFFIX = ".log";

  cid_t max_log_id_file = INVALID_CID;

  CopySerializeOutput output_buffer;

  int logger_id;

  cid_t max_delimiter_file = 0;

  bool should_create_new_file = false;

  TimePoint last_flush = Clock::now();

  Micros flush_frequency{peloton_flush_frequency_micros};
};

}  // namespace logging
}  // namespace peloton
