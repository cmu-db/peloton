
#pragma once

#include "type/serializeio.h"
#include "logging/log_record.h"

namespace peloton{
namespace logging{

class LogBuffer{

private:
  //TODO(gandeevan): Tune this size
  const static size_t log_buffer_threshold_ = 100;  // 4 KB

public:

  LogBuffer() : task_callback_(nullptr),
                task_callback_arg_(nullptr) {}

  ~LogBuffer() {}

  void WriteRecord(LogRecord &record);

  size_t GetSize() { return output_buffer.Size(); }

  static size_t GetThreshold() { return log_buffer_threshold_; }

  void SetTaskCallback(void (*task_callback)(void *), void *task_callback_arg) {
    task_callback_ = task_callback;
    task_callback_arg_ = task_callback_arg;
  }

private:
  CopySerializeOutput output_buffer;

  // The current callback to be invoked after logging completes.
  void (*task_callback_)(void *);
  void *task_callback_arg_;


};

}
}