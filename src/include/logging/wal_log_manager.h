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
#include "logging/log_buffer_pool.h"
#include "logging/log_manager.h"
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
 *  | txn_cid | database_id | table_id | operation_type | tilegroup and offset |data | ... | txn_end_flag
 *  -----------------------------------------------------------------------------
 *
 * NOTE: this layout is designed for physiological logging.
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */

class WalLogManager : public LogManager {
  WalLogManager(const WalLogManager &) = delete;
  WalLogManager &operator=(const WalLogManager &) = delete;
  WalLogManager(WalLogManager &&) = delete;
  WalLogManager &operator=(WalLogManager &&) = delete;

protected:

  WalLogManager()
    :is_running_(false) {}

public:
  static WalLogManager &GetInstance() {
    static WalLogManager log_manager;
    return log_manager;
  }
  virtual ~WalLogManager() {}

  virtual void SetDirectories(std::string logging_dir) override {
    logger_dir_ = logging_dir;
    // check the existence of logging directories.
    // if not exists, then create the directory.
      if (LoggingUtil::CheckDirectoryExistence(logging_dir.c_str()) == false) {
        LOG_INFO("Logging directory %s is not accessible or does not exist", logging_dir.c_str());
        bool res = LoggingUtil::CreateDirectory(logging_dir.c_str(), 0700);
        if (res == false) {
          LOG_ERROR("Cannot create directory: %s", logging_dir.c_str());
        }
      }

    logger_count_ = 1;
    for (size_t i = 0; i < logger_count_; ++i) {
      logger_ = new WalLogger(0, logging_dir);
    }
    buffer_ptr_ = new LogBuffer();
  }

  virtual const std::string GetDirectories() override {
    return logger_dir_;
  }

  LogBuffer* GetBuffer() {
      return buffer_ptr_;
  }


  void LogInsert(const ItemPointer &tuple_pos, cid_t current_cid);
  void LogUpdate(const ItemPointer &tuple_old_pos, const ItemPointer &tuple_pos, cid_t current_cid);
  void LogDelete(const ItemPointer &tuple_pos_deleted, cid_t current_cid);

  void StartPersistTxn(cid_t commit_id);
  void EndPersistTxn(cid_t commit_id);


  // Logger side logic
  virtual void DoRecovery();
  virtual void StartLoggers() override;
  virtual void StopLoggers() override;
private:

  void WriteRecordToBuffer(LogRecord &record);

private:
  std::string logger_dir_;

  CopySerializeOutput output_buffer_;

  LogBuffer* buffer_ptr_;

  WalLogger* logger_;

  bool is_running_;

};

}
}
