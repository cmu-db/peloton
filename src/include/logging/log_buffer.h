
#pragma once

#include "type/serializeio.h"
#include "logging/log_record.h"

namespace peloton{
namespace logging{


typedef void (*CallbackFunction)(void *); // function pointer type

class LogBuffer{

public:

  LogBuffer(size_t threshold)
          : log_buffer_threshold_(threshold),
            task_callback_(nullptr),
            task_callback_arg_(nullptr) {}

  ~LogBuffer() {}

  void WriteRecord(LogRecord &record);

  inline const char *GetData() { return log_buffer_.Data(); }
  inline size_t GetSize() { return log_buffer_.Size(); }


  inline CopySerializeOutput &GetCopySerializedOutput() { return log_buffer_; }

  inline bool HasThresholdExceeded() { return log_buffer_.Size() >= log_buffer_threshold_; }
  inline size_t GetThreshold() { return log_buffer_threshold_; }
  inline CallbackFunction GetCallback() { return task_callback_; }
  inline void* GetCallbackArgs() { return task_callback_arg_; }

  void SetTaskCallback(void (*task_callback)(void *), void *task_callback_arg) {
    task_callback_ = task_callback;
    task_callback_arg_ = task_callback_arg;
  }

private:
  CopySerializeOutput log_buffer_;
  size_t log_buffer_threshold_;

  // The current callback to be invoked after logging completes.
  void (*task_callback_)(void *);
  void *task_callback_arg_;


};

}
}