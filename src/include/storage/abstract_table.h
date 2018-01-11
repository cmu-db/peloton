//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_table.h
//
// Identification: src/include/storage/abstract_table.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <string>

#include "common/item_pointer.h"
#include "common/printable.h"
#include "common/internal_types.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

namespace peloton {

typedef std::map<oid_t, std::pair<oid_t, oid_t>> column_map_type;

namespace concurrency {
class TransactionContext;
}

namespace catalog {
class Manager;
class Schema;
}

namespace storage {

class Tuple;
class TileGroup;

/**
 * Base class for all tables
 */
class AbstractTable : public Printable {
 public:
  virtual ~AbstractTable();

 protected:
  // Table constructor
  AbstractTable(oid_t table_oid, catalog::Schema *schema, bool own_schema,
               peloton::LayoutType layout_type = peloton::LayoutType::ROW);

 public:
  //===--------------------------------------------------------------------===//
  // TUPLE OPERATIONS
  //===--------------------------------------------------------------------===//

  // insert tuple in table. the pointer to the index entry is returned as
  // index_entry_ptr.
  virtual ItemPointer InsertTuple(const Tuple *tuple,
                                  concurrency::TransactionContext *transaction,
                                  ItemPointer **index_entry_ptr = nullptr) = 0;

  // designed for tables without primary key. e.g., output table used by
  // aggregate_executor.
  virtual ItemPointer InsertTuple(const Tuple *tuple) = 0;

  //===--------------------------------------------------------------------===//
  // LAYOUT TYPE 
  //===--------------------------------------------------------------------===//

  void SetLayoutType(peloton::LayoutType layout) {
    layout_type = layout;
  }

  peloton::LayoutType GetLayoutType() {
    return layout_type;
  }
  //===--------------------------------------------------------------------===//
  // TILE GROUP
  //===--------------------------------------------------------------------===//

  // Offset is a 0-based number local to the table
  virtual std::shared_ptr<storage::TileGroup> GetTileGroup(
      const std::size_t &tile_group_offset) const = 0;

  // ID is the global identifier in the entire DBMS
  virtual std::shared_ptr<storage::TileGroup> GetTileGroupById(
      const oid_t &tile_group_id) const = 0;

  // Number of TileGroups that the table has
  virtual size_t GetTileGroupCount() const = 0;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  oid_t GetOid() const { return table_oid; }

  void SetSchema(catalog::Schema *given_schema) { schema = given_schema; }

  catalog::Schema *GetSchema() const { return (schema); }

  virtual std::string GetName() const = 0;

  // Get a string representation for debugging
  const std::string GetInfo() const;

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  virtual bool HasPrimaryKey() const = 0;

  virtual bool HasUniqueConstraints() const = 0;

  virtual bool HasForeignKeys() const = 0;

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  virtual void IncreaseTupleCount(const size_t &amount) = 0;

  virtual void DecreaseTupleCount(const size_t &amount) = 0;

  virtual void SetTupleCount(const size_t &num_tuples) = 0;

  virtual size_t GetTupleCount() const = 0;

 protected:
  //===--------------------------------------------------------------------===//
  // INTERNAL METHODS
  //===--------------------------------------------------------------------===//

  TileGroup *GetTileGroupWithLayout(oid_t database_id, oid_t tile_group_id,
                                    const column_map_type &partitioning,
                                    const size_t num_tuples);

  column_map_type GetTileGroupLayout() const;

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  oid_t table_oid;

  // table schema
  catalog::Schema *schema;

  /**
   * @brief Should this table own the schema?
   * Usually true.
   * Will be false if the table is for intermediate results within a query,
   * where the scheme may live longer.
   */
  bool own_schema_;

  peloton::LayoutType layout_type;
};

}  // namespace storage
}  // namespace peloton
