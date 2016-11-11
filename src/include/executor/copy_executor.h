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

#define COPY_BUFFER_SIZE 65536
#define INVALID_COL_ID -1

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

  // Initialize the column ids for query parameters
  void InitParamColIds();

  // Flush the local buffer
  void FlushBuffer();

  // Copy and escape the content of column to local buffer
  void Copy(const char *data, int len, bool end_of_line);

  // Create a packet for prepared statement parameter data before parsing it
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

  // New line delimiter between rows
  char new_line = '\n';

  // Total number of bytes written
  size_t total_bytes_written = 0;

  // The special column ids in query_metric table
  unsigned int num_param_col_id = INVALID_COL_ID;
  unsigned int param_type_col_id = INVALID_COL_ID;
  unsigned int param_format_col_id = INVALID_COL_ID;
  unsigned int param_val_col_id = INVALID_COL_ID;
};

}  // namespace executor
}  // namespace peloton
