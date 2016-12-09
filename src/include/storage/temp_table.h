
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// temp_table.h
//
// Identification: /peloton/src/include/storage/temp_table.h
//
// Copyright (c) 2015, 2016 Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/logger.h"
#include "common/macros.h"
#include "storage/abstract_table.h"

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {

//===--------------------------------------------------------------------===//
// TempTable
//===--------------------------------------------------------------------===//

/**
 * A TempTable is a non-thread-safe place to store tuples that don't
 * need to be durable, don't need indexes, don't need constraints.
 * This is designed to be faster than DataTable.
 */
class TempTable : public AbstractTable {
 public:
  virtual ~TempTable();

 protected:
  // Table constructor
  TempTable(const oid_t table_oid, catalog::Schema *schema,
            const bool own_schema);

  //===--------------------------------------------------------------------===//
  // TUPLE OPERATIONS
  //===--------------------------------------------------------------------===//

  // insert tuple in table. the pointer to the index entry is returned as
  // index_entry_ptr.
  ItemPointer InsertTuple(const Tuple *tuple,
                          concurrency::Transaction *transaction,
                          ItemPointer **index_entry_ptr = nullptr);

  // designed for tables without primary key. e.g., output table used by
  // aggregate_executor.
  ItemPointer InsertTuple(const Tuple *tuple);

  //===--------------------------------------------------------------------===//
  // TILE GROUP
  //===--------------------------------------------------------------------===//

  // Offset is a 0-based number local to the table
  inline std::shared_ptr<storage::TileGroup> GetTileGroup(
      const std::size_t &tile_group_offset) const {
    PL_ASSERT(tile_group_offset < GetTileGroupCount());
    return (tile_groups_[tile_group_offset]));
  }

  inline std::shared_ptr<storage::TileGroup> GetTileGroupById(
      const oid_t &tile_group_id) const {
    for (auto tg : tile_groups_) {
      if (tg->GetTileGroupId() == tile_group_id) {
        return (tg);
      }
    }
    LOG_TRACE("No TileGroup with id %d exists in %s", tile_group_id,
              this->GetName().c_str());
    return (nullptr);
  }

  // Number of TileGroups that the table has
  inline size_t GetTileGroupCount() const { return (tile_groups_.size()); }

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  std::string GetName() const {
    std::ostringstream os;
    os << "TEMP_TABLE[" << table_oid << "]";
    return (os.str());
  }

  // Get a string representation for debugging
  inline const std::string GetInfo() const { this->GetInfo(); }

  inline bool HasPrimaryKey() const {return (false)};

  inline bool HasUniqueConstraints() const {return (false)};

  inline bool HasForeignKeys() const {return (false)};

 private:
  // This is where we're actually going to store the data for this table
  std::vector<std::shared_ptr<storage::TileGroup>> tile_groups_;
};

}  // End storage namespace
}  // End peloton namespace
