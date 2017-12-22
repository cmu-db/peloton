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

class Tuple;
class TileGroup;
class TileGroupHeader;

// FIXME
const size_t TEMPTABLE_DEFAULT_SIZE = 100;  // # of tuples

// FIXME
const oid_t TEMPTABLE_TILEGROUP_ID = 99999;

//===--------------------------------------------------------------------===//
// TempTable
//===--------------------------------------------------------------------===//

/**
 * A TempTable is a non-thread-safe place to store tuples that don't
 * need to be durable, don't need indexes, don't need constraints.
 * This is designed to be faster than DataTable.
 */
class TempTable : public AbstractTable {
  TempTable() = delete;

 public:
  // Table constructor
  TempTable(const oid_t &table_oid, catalog::Schema *schema,
            const bool own_schema,
            const peloton::LayoutType layout_type = peloton::LayoutType::ROW);

  ~TempTable();

  //===--------------------------------------------------------------------===//
  // TUPLE OPERATIONS
  //===--------------------------------------------------------------------===//

  // insert tuple in table. the pointer to the index entry is returned as
  // index_entry_ptr.
  ItemPointer InsertTuple(const Tuple *tuple,
                          concurrency::TransactionContext *transaction,
                          ItemPointer **index_entry_ptr = nullptr) override;

  // designed for tables without primary key. e.g., output table used by
  // aggregate_executor.
  ItemPointer InsertTuple(const Tuple *tuple) override;

  //===--------------------------------------------------------------------===//
  // TILE GROUP
  //===--------------------------------------------------------------------===//

  // Offset is a 0-based number local to the table
  std::shared_ptr<storage::TileGroup> GetTileGroup(
      const std::size_t &tile_group_offset) const override ;

  std::shared_ptr<storage::TileGroup> GetTileGroupById(
      const oid_t &tile_group_id) const override;

  // Number of TileGroups that the table has
  inline size_t GetTileGroupCount() const override {
    return tile_groups_.size();
  }

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  std::string GetName() const override;

  inline bool HasPrimaryKey() const override { return (false); }

  inline bool HasUniqueConstraints() const override { return (false); }

  inline bool HasForeignKeys() const override { return (false); }

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  inline void IncreaseTupleCount(const size_t &amount) override {
    number_of_tuples_ += amount;
  }

  inline void DecreaseTupleCount(const size_t &amount) override {
    number_of_tuples_ -= amount;
  }

  inline void SetTupleCount(const size_t &num_tuples) override {
    number_of_tuples_ = num_tuples;
  }

  inline size_t GetTupleCount() const override { return number_of_tuples_; }

 protected:
  //===--------------------------------------------------------------------===//
  // INTERNAL METHODS
  //===--------------------------------------------------------------------===//

  oid_t AddDefaultTileGroup();

 private:
  // This is where we're actually going to store the data for this table
  std::vector<std::shared_ptr<storage::TileGroup>> tile_groups_;

  // Counter for the # of tuples that this TempTable currently holds.
  // We don't need to protect this because a TempTable is not meant to be
  // concurrent.
  size_t number_of_tuples_ = 0;
};

}  // namespace storage
}  // namespace peloton
