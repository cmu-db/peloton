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

#include "backend/catalog/catalog.h"

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

  // TODO: Fix function
  static bool Execute();

 protected:

  static bool CreateDatabase(std::string db_name);

  // TODO: Fix function
  static bool CreateTable(catalog::Database* db);

  static bool CreateIndex(catalog::Database* db,
                          std::string index_name,
                          std::string table_name,
                          std::vector<std::string> index_attrs,
                          bool unique);


  static bool CreateConstraint(catalog::Database* db,
                               std::string table_name);

};

} // namespace executor
} // namespace nstore

