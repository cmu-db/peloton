//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// manager.h
//
// Identification: src/include/catalog/manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <utility>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <memory>

#include "common/macros.h"
#include "common/types.h"
#include "container/cuckoo_map.h"
#include "container/lock_free_array.h"

namespace peloton {

namespace storage {
class DataTable;
class Database;
class TileGroup;
}
namespace index {
class Index;
}

namespace catalog {

//===--------------------------------------------------------------------===//
// Manager
//===--------------------------------------------------------------------===//

class Manager {
 public:
  Manager() {}

  // Singleton
  static Manager &GetInstance();

  //===--------------------------------------------------------------------===//
  // OBJECT MAP
  //===--------------------------------------------------------------------===//

  oid_t GetNextOid() { return ++oid; }

  oid_t GetCurrentOid() { return oid; }

  void SetNextOid(oid_t next_oid) { oid = next_oid; }

  void AddTileGroup(const oid_t oid,
                    std::shared_ptr<storage::TileGroup> location);

  void DropTileGroup(const oid_t oid);

  std::shared_ptr<storage::TileGroup> GetTileGroup(const oid_t oid);

  void ClearTileGroup(void);

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

  Manager(Manager const &) = delete;

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  std::atomic<oid_t> oid = ATOMIC_VAR_INIT(START_OID);

  LockFreeArray<std::shared_ptr<storage::TileGroup>> locator;

  // DATABASES

  std::vector<storage::Database *> databases;

  std::mutex catalog_mutex;

  static std::shared_ptr<storage::TileGroup> empty_location;
};

}  // End catalog namespace
}  // End peloton namespace
