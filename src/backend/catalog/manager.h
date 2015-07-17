/*-------------------------------------------------------------------------
 *
 * catalog.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/catalog.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <atomic>
#include <utility>
#include <mutex>
#include <vector>

#include "tbb/concurrent_unordered_map.h"
#include "backend/common/types.h"

namespace peloton {

namespace storage{
class DataTable;
class Database;
}
namespace index{
class Index;
}

namespace catalog {

//===--------------------------------------------------------------------===//
// Manager
//===--------------------------------------------------------------------===//

typedef tbb::concurrent_unordered_map<oid_t, void*> lookup_dir;

class Manager {

 public:
  Manager() {}

  // Singleton
  static Manager& GetInstance();

  //===--------------------------------------------------------------------===//
  // OBJECT MAP
  //===--------------------------------------------------------------------===//

  oid_t GetNextOid() {
    return ++oid;
  }

  oid_t GetCurrentOid() {
    return oid;
  }

  void SetLocation(const oid_t oid, void *location);

  void *GetLocation(const oid_t oid) const;

  //===--------------------------------------------------------------------===//
  // DATABASE
  //===--------------------------------------------------------------------===//

  void AddDatabase(storage::Database *database);

  storage::Database *GetDatabase(const oid_t database_offset) const;

  storage::Database *GetDatabaseWithOid(const oid_t database_oid) const;

  oid_t GetDatabaseCount() const;

  void DropDatabaseWithOid(const oid_t database_oid);

  //===--------------------------------------------------------------------===//
  // CONVENIENCE WRAPPERS
  //===--------------------------------------------------------------------===//

  // Look up the table
  storage::DataTable *GetTableWithOid(const oid_t database_oid,
                                     const oid_t table_oid) const;

  storage::DataTable *GetTableWithName(const oid_t database_oid,
                                       const std::string table_name) const;

  // Look up the index
  index::Index *GetIndexWithOid(const oid_t database_oid,
                                const oid_t table_oid,
                                const oid_t index_oid) const;

  Manager(Manager const&) = delete;

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  std::atomic<oid_t> oid = ATOMIC_VAR_INIT(START_OID);

  lookup_dir locator;

  // DATABASES

  std::vector<storage::Database*> databases;

  std::mutex catalog_mutex;

};

} // End catalog namespace
} // End peloton namespace
