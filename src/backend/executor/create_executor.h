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
#include "backend/catalog/schema.h"
#include "backend/bridge/ddl.h"

namespace peloton {
namespace executor {

//===--------------------------------------------------------------------===//
// Create Statements
//===--------------------------------------------------------------------===//

class CreateExecutor {
 friend class bridge::DDL; 
 
 public:

  CreateExecutor(const CreateExecutor &) = delete;
  CreateExecutor& operator=(const CreateExecutor &) = delete;
  CreateExecutor(CreateExecutor &&) = delete;
  CreateExecutor& operator=(CreateExecutor &&) = delete;

  // TODO: Fix function
  static bool Execute(std::string name, CreateType createType);

 protected:

  static bool CreateDatabase(std::string db_name);

  // TODO: Fix function
  static bool CreateTable(catalog::Database* db, std::string table_name,
                          DDL_ColumnInfo* columnInfo, int num_columns, catalog::Schema* schema );

  static bool CreateIndex(catalog::Database* db,
                          std::string index_name,
                          std::string table_name,
                          std::vector<std::string> index_attrs,
                          bool unique);


  static bool CreateConstraint(catalog::Database* db,
                               std::string table_name);

};

} // namespace executor
} // namespace peloton

