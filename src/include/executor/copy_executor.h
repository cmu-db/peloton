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

 protected:
  bool DInit();

  bool DExecute();

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Computed the result */
  bool done = false;

  // The handler for the output file
  FileHandle file_handle_ = INVALID_FILE_HANDLE;

  char delimiter = ',';
};

}  // namespace executor
}  // namespace peloton
