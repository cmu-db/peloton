
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.cpp
//
// Identification: src/catalog/catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "catalog/catalog_storage_manager.h"

#include <iostream>

#include "catalog/database_metrics_catalog.h"
#include "catalog/manager.h"
#include "catalog/query_metrics_catalog.h"
#include "catalog/table_metrics_catalog.h"
#include "catalog/index_metrics_catalog.h"
#include "common/exception.h"
#include "common/macros.h"
#include "expression/date_functions.h"
#include "expression/string_functions.h"
#include "expression/decimal_functions.h"
#include "index/index_factory.h"
#include "util/string_util.h"

namespace peloton {
namespace catalog {

// Get instance of the global catalog storage manager
CatalogStorageManager *CatalogStorageManager::GetInstance(void) {
  static std::unique_ptr<CatalogStorageManager> global_catalog_storage_manager(new CatalogStorageManager());
  return global_catalog_storage_manager.get();
}

CatalogStorageManager::CatalogStorageManager() {

}


//===--------------------------------------------------------------------===//
// GET WITH OID - DIRECTLY GET FROM STORAGE LAYER
//===--------------------------------------------------------------------===//

/* Find a database using its oid from storage layer,
 * throw exception if not exists
 * */
storage::Database *CatalogStorageManager::GetDatabaseWithOid(oid_t database_oid) const {
  for (auto database : databases_)
    if (database->GetOid() == database_oid) return database;
  throw CatalogException("Database with oid = " + std::to_string(database_oid) +
                         " is not found");
  return nullptr;
}

/* Find a table using its oid from storage layer,
 * throw exception if not exists
 * */
storage::DataTable *CatalogStorageManager::GetTableWithOid(oid_t database_oid,
                                             oid_t table_oid) const {
  LOG_TRACE("Getting table with oid %d from database with oid %d", database_oid,
            table_oid);
  // Lookup DB from storage layer
  auto database =
      GetDatabaseWithOid(database_oid);  // Throw exception if not exists
  // Lookup table from storage layer
  return database->GetTableWithOid(table_oid);  // Throw exception if not exists
}

/* Find a index using its oid from storage layer,
 * throw exception if not exists
 * */
index::Index *CatalogStorageManager::GetIndexWithOid(oid_t database_oid, oid_t table_oid,
                                       oid_t index_oid) const {
  // Lookup table from storage layer
  auto table = GetTableWithOid(database_oid,
                               table_oid);  // Throw exception if not exists
  // Lookup index from storage layer
  return table->GetIndexWithOid(index_oid)
      .get();  // Throw exception if not exists
}

//===--------------------------------------------------------------------===//
// DEPRECATED
//===--------------------------------------------------------------------===//

// This is used as an iterator
storage::Database *CatalogStorageManager::GetDatabaseWithOffset(oid_t database_offset) const {
  PL_ASSERT(database_offset < databases_.size());
  auto database = databases_.at(database_offset);
  return database;
}


//===--------------------------------------------------------------------===//
// HELPERS
//===--------------------------------------------------------------------===//

// Only used for testing
bool CatalogStorageManager::HasDatabase(oid_t db_oid) const {
  for (auto database : databases_)
    if (database->GetOid() == db_oid) return (true);
  return (false);
}

void CatalogStorageManager::DestroyDatabases() {
    LOG_TRACE("Deleting databases");
    for (auto database : databases_) delete database;
    LOG_TRACE("Finish deleting database");
}

CatalogStorageManager::~CatalogStorageManager() {

}
}
}
