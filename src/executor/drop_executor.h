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

#include "catalog/catalog.h"
#include "parser/parser.h"

namespace nstore {
namespace executor {

//===--------------------------------------------------------------------===//
// Drop Statements
//===--------------------------------------------------------------------===//

class DropExecutor {

 public:

  static bool Execute(parser::SQLStatement *query);

};

} // namespace executor
} // namespace nstore
