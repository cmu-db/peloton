//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// manager.h
//
// Identification: src/backend/catalog/manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <utility>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <memory>

#include "backend/common/types.h"

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

typedef std::unordered_map<oid_t, std::shared_ptr<storage::TileGroup > > lookup_dir;

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

  void AddTileGroup(const oid_t oid, const std::shared_ptr<storage::TileGroup>& location);

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

  // Look up the index
  index::Index *GetIndexWithOid(const oid_t database_oid, const oid_t table_oid,
                                const oid_t index_oid) const;

  Manager(Manager const &) = delete;

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  std::atomic<oid_t> oid = ATOMIC_VAR_INIT(START_OID);

  lookup_dir locator;

  std::mutex locator_mutex;

  // DATABASES

  std::vector<storage::Database *> databases;

  std::mutex catalog_mutex;
};

}  // End catalog namespace
}  // End peloton namespace
