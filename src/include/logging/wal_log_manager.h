//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_log_manager.h
//
// Identification: src/include/logging/wal_log_manager.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/log_record.h"
#include "type/types.h"
#include "common/logger.h"

namespace peloton {
namespace logging {

/**
 * logging file name layout :
 *
 * dir_name + "/" + prefix + "_" + 0
 *
 *
 * logging file layout :
 *
 *  -----------------------------------------------------------------------------
 *  | txn_eid | txn_cid | database_id | table_id | operation_type | tilegroup
 *and offset |data | ... |
 *  -----------------------------------------------------------------------------
 *
 * NOTE: this layout is designed for physiological logging.
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */


class WalLogManager {
  WalLogManager(const WalLogManager &) = delete;
  WalLogManager &operator=(const WalLogManager &) = delete;
  WalLogManager(WalLogManager &&) = delete;
  WalLogManager &operator=(WalLogManager &&) = delete;

 public:
  WalLogManager() {}

  // Constructor. Same fashion as tcop
  WalLogManager(void (*task_callback)(void *), void *task_callback_arg)
      : task_callback_(task_callback), task_callback_arg_(task_callback_arg) {}
  ~WalLogManager() {}

  static void SetDirectory(std::string logging_dir);

  static void WriteTransactionWrapper(void *args);

  static void WriteTransaction(std::vector<LogRecord> log_records);

  // Logger side logic
  static void DoRecovery();

  ResultType LogTransaction(std::vector<LogRecord> log_records);

  void SetTaskCallback(void (*task_callback)(void *), void *task_callback_arg) {
    task_callback_ = task_callback;
    task_callback_arg_ = task_callback_arg;
  }
  bool is_running_ = false;
 private:
  void (*task_callback_)(void *);
  void *task_callback_arg_;


};


static std::string log_directory_;

struct LogTransactionArg {
  inline LogTransactionArg(const std::vector<LogRecord> log_records)
      : log_records_(log_records) {}

  std::vector<LogRecord> log_records_;
};
}
}
