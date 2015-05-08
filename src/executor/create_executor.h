/*-------------------------------------------------------------------------
 *
 * create_executor.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/create_executor.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/catalog.h"
#include "parser/parser.h"

namespace nstore {
namespace executor {

//===--------------------------------------------------------------------===//
// Create Statements
//===--------------------------------------------------------------------===//

class CreateExecutor {
 public:

  CreateExecutor(const CreateExecutor &) = delete;
  CreateExecutor& operator=(const CreateExecutor &) = delete;
  CreateExecutor(CreateExecutor &&) = delete;
  CreateExecutor& operator=(CreateExecutor &&) = delete;

  static bool Execute(parser::SQLStatement *query);

};

} // namespace executor
} // namespace nstore

