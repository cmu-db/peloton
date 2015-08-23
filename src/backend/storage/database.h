//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// database.h
//
// Identification: src/backend/storage/database.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/storage/data_table.h"

#include <iostream>

struct Peloton_Status;
struct dirty_table_info;
struct dirty_index_info;

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// DATABASE
//===--------------------------------------------------------------------===//

class Database {
 public:
  Database(Database const &) = delete;

  Database(oid_t database_oid) : database_oid(database_oid) {}

  ~Database();

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

  void UpdateStats(Peloton_Status *status, bool dirty_care);

  void UpdateStatsWithOid(Peloton_Status *status, const oid_t table_oid);

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  static dirty_table_info **CreateDirtyTables(
      std::vector<dirty_table_info *> dirty_tables_vec);

  static dirty_index_info **CreateDirtyIndexes(
      std::vector<dirty_index_info *> dirty_indexes_vec);

  static dirty_table_info *CreateDirtyTable(oid_t table_oid,
                                            float number_of_tuples,
                                            dirty_index_info **dirty_indexes,
                                            oid_t index_count);

  static dirty_index_info *CreateDirtyIndex(oid_t index_oid,
                                            float number_of_tuples);

  // Get a string representation of this database
  friend std::ostream &operator<<(std::ostream &os, const Database &database);

 protected:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // database oid
  oid_t database_oid;

  // TABLES
  std::vector<storage::DataTable *> tables;

  std::mutex database_mutex;
};

}  // End storage namespace
}  // End peloton namespace
