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
#include "backend/parser/parser.h"

namespace nstore {
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

  static bool Execute(parser::SQLStatement *query);

};

} // namespace executor
} // namespace nstore
