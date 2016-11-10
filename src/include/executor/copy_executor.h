//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.h
//
// Identification: src/include/executor/copy_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"

#include <vector>
#include "wire/wire.h"

//#define COPY_BUFFER_SIZE 16384
#define COPY_BUFFER_SIZE 4096

namespace peloton {
namespace executor {

class CopyExecutor : public AbstractExecutor {
 public:
  CopyExecutor(const CopyExecutor &) = delete;
  CopyExecutor &operator=(const CopyExecutor &) = delete;
  CopyExecutor(CopyExecutor &&) = delete;
  CopyExecutor &operator=(CopyExecutor &&) = delete;

  CopyExecutor(const planner::AbstractPlan *node,
               ExecutorContext *executor_context);

  ~CopyExecutor();

  inline size_t GetTotalBytesWritten() { return total_bytes_written; }

 protected:
  bool DInit();

  bool DExecute();

  void Copy(const char *data, int len, bool end_of_line);

  void CreateParamPacket(wire::Packet &packet, int len, std::string &val);

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Computed the result */
  bool done = false;

  // Internal copy buffer
  char buff[COPY_BUFFER_SIZE];

  // The size of data to flush
  size_t buff_size = 0;

  // Pointer in the buffer
  size_t buff_ptr = 0;

  // The handler for the output file
  FileHandle file_handle_ = INVALID_FILE_HANDLE;

  // Field delimiter between columns
  char delimiter = ',';

  // Whether we're copying the parameters which require deserialization
  bool deserialize_parameters = false;

  // Total number of bytes written
  size_t total_bytes_written = 0;
};

}  // namespace executor
}  // namespace peloton
