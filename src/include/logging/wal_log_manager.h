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
 * dir_name + "/" + prefix + "_" + epoch_id
 *
 *
 * logging file layout :
 *
 *  -----------------------------------------------------------------------------
 *  | txn_eid | txn_cid | database_id | table_id | operation_type | tilegroup and offset |data | ... | txn_end_flag
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
  WalLogManager()
    :is_running_(false) {}
  WalLogManager(void(* task_callback)(void *), void *task_callback_arg):
      task_callback_(task_callback), task_callback_arg_(task_callback_arg),is_running_(false) {
  }
  /*static WalLogManager &GetInstance() {
    static WalLogManager log_manager;
    return log_manager;
  }*/
  ~WalLogManager() {}

  static void SetDirectories(std::string logging_dir);

  static void WriteTransactionWrapper(void* args);

  static void WriteTransaction(std::vector<LogRecord> log_records, ResultType* result);

  // Logger side logic
  static void DoRecovery();

  ResultType LogTransaction(std::vector<LogRecord> log_records);

  void SetTaskCallback(void(* task_callback)(void*), void *task_callback_arg) {
    task_callback_ = task_callback;
    task_callback_arg_ = task_callback_arg;
  }

private:
  void(* task_callback_)(void *);
  void * task_callback_arg_;
  bool is_running_;

};


struct LogTransactionArg {
  inline LogTransactionArg(const std::vector<LogRecord> log_records,
                        ResultType* p_status) :
      log_records_(log_records),
      p_status_(p_status) {}
//      event_(event) {}
//      io_trigger_(io_trigger) { }


  std::vector<LogRecord> log_records_;
  ResultType *p_status_;
//  struct event* event_;
//  IOTrigger *io_trigger_;
};
}
}
