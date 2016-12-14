
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// temp_table.cpp
//
// Identification: /peloton/src/storage/temp_table.cpp
//
// Copyright (c) 2015, 2016 Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/temp_table.h"

#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"

namespace peloton {
namespace storage {

TempTable::TempTable(const oid_t &table_oid, catalog::Schema *schema,
                     const bool own_schema)
    : AbstractTable(table_oid, schema, own_schema) {
  // Nothing to see, nothing to do
}

TempTable::~TempTable() {
  // Nothing to see, nothing to do
}

ItemPointer TempTable::InsertTuple(const Tuple *tuple,
                                   concurrency::Transaction *transaction,
                                   ItemPointer **index_entry_ptr) {
  PL_ASSERT(tuple != nullptr);
  PL_ASSERT(transaction == nullptr);
  PL_ASSERT(index_entry_ptr == nullptr);

  // FIXME
  ItemPointer ptr(INVALID_OID, INVALID_OID);
  return (ptr);
}
ItemPointer TempTable::InsertTuple(const Tuple *tuple) {
  return (this->InsertTuple(tuple, nullptr, nullptr));
}

std::shared_ptr<storage::TileGroup> TempTable::GetTileGroup(
    const std::size_t &tile_group_offset) const {
  PL_ASSERT(tile_group_offset < GetTileGroupCount());
  return (tile_groups_[tile_group_offset]);
}

std::shared_ptr<storage::TileGroup> TempTable::GetTileGroupById(
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

}  // End storage namespace
}  // End peloton namespace
