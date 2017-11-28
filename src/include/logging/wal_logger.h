//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_logger.h
//
// Identification: src/include/logging/wal_logger.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "logging/wal_log_manager.h"
#include "type/serializeio.h"
#include "type/types.h"
#include "common/logger.h"

namespace peloton {

namespace storage {
class TileGroupHeader;
}

namespace logging {

class WalLogger {
 public:
  WalLogger(const size_t &log_id, const std::string &log_dir)
      : log_id_(log_id), log_dir_(log_dir) {}

  ~WalLogger() {}

  void WriteTransaction(std::vector<LogRecord> log_records);
  void PersistLogBuffer(LogBuffer *log_buffer);

 private:
  std::string GetLogFileFullPath() {
    return log_dir_ + "/" + logging_filename_prefix_ + "_" +
           std::to_string(log_id_);
  }

  CopySerializeOutput *WriteRecordToBuffer(LogRecord &record);

 private:
  size_t log_id_;
  std::string log_dir_;

  const std::string logging_filename_prefix_ = "logfile";
};
}
}
