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

#include "tbb/concurrent_unordered_map.h"
#include "backend/common/types.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace catalog {

class Database;

//===--------------------------------------------------------------------===//
// Manager
//===--------------------------------------------------------------------===//

typedef tbb::concurrent_unordered_map<oid_t, void*> lookup_dir;

class Manager {

 public:
  Manager() {}

  // Singleton
  static Manager& GetInstance();

  oid_t GetNextOid() {
    return ++oid;
  }

  oid_t GetOid() {
    return oid;
  }

  void SetLocation(const oid_t oid, void *location);

  void *GetLocation(const oid_t oid) const;

  // Look up the database
  catalog::Database *GetDatabase(const oid_t database_id) const;

  // Look up the table
  catalog::Table *GetTable(const oid_t database_id,
                           const oid_t table_id) const;

  // Look up the index
  catalog::Index *GetIndex(const oid_t database_id,
                           const oid_t table_id,
                           const oid_t index_id) const;

  // Look up the schema
  catalog::Schema *GetSchema(const oid_t database_id,
                             const oid_t table_id) const;

  Manager(Manager const&) = delete;

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  std::atomic<oid_t> oid = ATOMIC_VAR_INIT(START_OID);

  lookup_dir locator;

};

} // End catalog namespace
} // End peloton namespace
