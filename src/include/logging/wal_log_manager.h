//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reordered_phylog_log_manager.h
//
// Identification: src/backend/logging/reordered_phylog_log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <list>
#include <stack>

#include "concurrency/transaction.h"
#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "logging/logging_util.h"
#include "logging/wal_logger.h"
#include "type/types.h"
#include "type/serializer.h"
#include "container/lock_free_queue.h"
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
