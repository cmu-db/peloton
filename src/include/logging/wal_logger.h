//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reordered_phylog_logger.h
//
// Identification: src/backend/logging/reordered_phylog_logger.h
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
#include "concurrency/epoch_manager.h"
#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "logging/wal_log_manager.h"
#include "type/types.h"
#include "type/serializer.h"
#include "container/lock_free_queue.h"
#include "common/logger.h"
#include "type/abstract_pool.h"

namespace peloton {

namespace storage {
class TileGroupHeader;
}

namespace logging {

class WalLogger {
 public:
  WalLogger(const size_t &logger_id, const std::string &log_dir)
      : logger_id_(logger_id), log_dir_(log_dir) {}

  ~WalLogger() {}

  void WriteTransaction(std::vector<LogRecord> log_records);
  void PersistLogBuffer(LogBuffer *log_buffer);

 private:
  // void Run();

  std::string GetLogFileFullPath(size_t epoch_id) {
    return log_dir_ + "/" + logging_filename_prefix_ + "_" +
           std::to_string(logger_id_) + "_" + std::to_string(epoch_id);
  }

  void GetSortedLogFileIdList();

  bool ReplayLogFile(FileHandle &file_handle);

  CopySerializeOutput *WriteRecordToBuffer(LogRecord &record);

 private:
  size_t logger_id_;
  std::string log_dir_;

  // recovery threads
  std::vector<size_t> file_eids_;
  std::atomic<int> max_replay_file_id_;

  /* Recovery */
  // TODO: Check if we can discard the recovery pool after the recovery is done.
  // Since every thing is copied to the
  // tile group and tile group related pool

  // logger thread

  /* File system related */
  //  CopySerializeOutput logger_output_buffer_;

  /* Log buffers */
  //  LockFreeQueue<CopySerializeOutput*> log_buffers_{100000};

  size_t persist_epoch_id_;

  // The spin lock to protect the worker map. We only update this map when
  // creating/terminating a new worker
  // Spinlock buffers_lock_;
  // map from worker id to the worker's context.
  // std::unordered_map<oid_t, std::shared_ptr<WorkerContext>> worker_map_;
  // LogBuffer* log_buffer_ = new LogBuffer();

  const std::string logging_filename_prefix_ = "log";
};
}
}
