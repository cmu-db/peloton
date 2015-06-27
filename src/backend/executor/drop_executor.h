/*-------------------------------------------------------------------------
 *
 * drop_executor.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/drop_executor.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/catalog.h"

namespace peloton {
namespace executor {

//===--------------------------------------------------------------------===//
// Drop Statements
//===--------------------------------------------------------------------===//

class DropExecutor {

 public:

  DropExecutor(const DropExecutor &) = delete;
  DropExecutor& operator=(const DropExecutor &) = delete;
  DropExecutor(DropExecutor &&) = delete;
  DropExecutor& operator=(DropExecutor &&) = delete;

  // TODO: Fix function
  static bool Execute();

};

} // namespace executor
} // namespace peloton
