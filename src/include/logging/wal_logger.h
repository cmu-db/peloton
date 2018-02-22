//
// Created by Gandeevan R on 2/12/18.
//

#pragma once

#include <deque>

#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "logging/wal_log_manager.h"
#include "type/serializeio.h"


namespace peloton {

namespace storage {
    class TileGroupHeader;
}

namespace logging {



class WalLogger {
private:
    struct logger_callback{

    };

public:

  WalLogger() {
      disk_buffer_ = new LogBuffer(LogManager::GetLoggerBufferSize());
  }

  ~WalLogger() {}

  bool IsFlushNeeded(bool pending_buffers);
  void FlushToDisk();
  void AppendLogBuffer(LogBuffer *buffer);

private:
  LogBuffer *disk_buffer_;
  std::deque<std::pair<CallbackFunction, void *>> callbacks_;

};
}
}


