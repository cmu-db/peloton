//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer.h
//
// Identification: src/include/logging/log_buffer.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "type/serializeio.h"
#include "logging/log_record.h"

namespace peloton{
namespace logging{

using LoggerCallback = std::function<void()>;

class LogBuffer{

public:

  LogBuffer(size_t threshold)
          : log_buffer_threshold_(threshold),
            on_flush_(nullptr) {}

  ~LogBuffer() {}

  void WriteRecord(LogRecord &record);

  inline const char *GetData() { return log_buffer_.Data(); }
  inline size_t GetSize() { return log_buffer_.Size(); }


  inline CopySerializeOutput &GetCopySerializedOutput() { return log_buffer_; }

  inline bool HasThresholdExceeded() { return log_buffer_.Size() >= log_buffer_threshold_; }
  inline size_t GetThreshold() { return log_buffer_threshold_; }


  inline void SetLoggerCallback(const LoggerCallback &on_flush) {
    on_flush_ = std::move(on_flush);
  }

  inline LoggerCallback &GetLoggerCallback() {
    return on_flush_;
  }

private:
  CopySerializeOutput log_buffer_;
  size_t log_buffer_threshold_;
  // The current callback to be invoked after logging completes.
  LoggerCallback on_flush_;
};

}
}