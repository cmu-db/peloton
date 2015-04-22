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

// DML processor
class CreateExecutor {

 public:

  static bool Execute(parser::SQLStatement *query);

 private:

  // created default db ?
  static bool has_default_db;

  // default db
  static catalog::CatalogType *cluster;
};

} // namespace executor
} // namespace nstore

