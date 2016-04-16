//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database.h
//
// Identification: src/backend/storage/database.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>

#include "backend/common/printable.h"
#include "backend/storage/data_table.h"
#include "backend/gc/gc_manager_factory.h"

struct peloton_status;
struct dirty_table_info;
struct dirty_index_info;

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// DATABASE
//===--------------------------------------------------------------------===//

class Database : public Printable {
 public:
  Database() = delete;
  Database(Database const &) = delete;

  Database(oid_t database_oid) : database_oid(database_oid) {
    gc_manager.reset(gc::GCManagerFactory::CreateInstance());
    gc_manager->StartGC();
  }

  ~Database(){
    // Clean up all the tables
    for (auto table : tables) delete table;

    gc_manager->StopGC();
  }

  //===--------------------------------------------------------------------===//
  // OPERATIONS
  //===--------------------------------------------------------------------===//

  oid_t GetOid() const { return database_oid; }

  //===--------------------------------------------------------------------===//
  // TABLE
  //===--------------------------------------------------------------------===//

  void AddTable(storage::DataTable *table);

  storage::DataTable *GetTable(const oid_t table_offset) const;

  storage::DataTable *GetTableWithOid(const oid_t table_oid) const;

  storage::DataTable *GetTableWithName(const std::string table_name) const;

  oid_t GetTableCount() const;

  void DropTableWithOid(const oid_t table_oid);

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  void UpdateStats(void) const;

  void UpdateStatsWithOid(const oid_t table_oid) const;

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

 protected:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // database oid
  oid_t database_oid;

  // TABLES
  std::vector<storage::DataTable *> tables;

  std::mutex database_mutex;

  std::shared_ptr<gc::GCManager> gc_manager;

};

}  // End storage namespace
}  // End peloton namespace
