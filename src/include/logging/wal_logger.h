//
// Created by Gandeevan R on 2/12/18.
//

#pragma once

#include <deque>

#include "common/container/lock_free_queue.h"
#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "logging/wal_log_manager.h"
#include "type/serializeio.h"


namespace peloton {
namespace logging {

using LogToken = peloton::ProducerToken;

class WalLogger {
public:


  WalLogger() {
      disk_buffer_ = new LogBuffer(LogManager::GetLoggerBufferSize());
  }

  ~WalLogger() {}

  bool IsFlushNeeded(bool pending_buffers);
  void FlushToDisk();
  void PerformCompaction(LogBuffer *buffer);

private:
  LogBuffer *disk_buffer_;
  std::deque<LoggerCallback> callbacks_;
};
}
}
