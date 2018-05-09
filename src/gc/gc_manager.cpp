//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager.cpp
//
// Identification: src/gc/gc_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "gc/gc_manager.h"

#include "catalog/schema.h"
#include "common/internal_types.h"
#include "concurrency/transaction_context.h"
#include "type/value.h"
#include "type/abstract_pool.h"
#include "storage/tile.h"
#include "storage/tile_group.h"

namespace peloton {
namespace gc {

// Check a tuple and reclaim all varlen field
void GCManager::CheckAndReclaimVarlenColumns(storage::TileGroup *tile_group,
                                             oid_t tuple_id) {
  uint32_t tile_count = tile_group->tile_count_;
  uint32_t tile_col_count;
  type::TypeId type_id;
  char *tuple_location;
  char *field_location;
  char *varlen_ptr;

  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    storage::Tile *tile = tile_group->GetTile(tile_itr);
    PELOTON_ASSERT(tile);
    const catalog::Schema *schema = tile->GetSchema();
    tile_col_count = schema->GetColumnCount();
    for (oid_t tile_col_itr = 0; tile_col_itr < tile_col_count;
         ++tile_col_itr) {
      type_id = schema->GetType(tile_col_itr);

      if ((type_id != type::TypeId::VARCHAR &&
           type_id != type::TypeId::VARBINARY) ||
          (schema->IsInlined(tile_col_itr) == true)) {
        // Not of varlen type, or is inlined, skip
        continue;
      }
      // Get the raw varlen pointer
      tuple_location = tile->GetTupleLocation(tuple_id);
      field_location = tuple_location + schema->GetOffset(tile_col_itr);
      varlen_ptr = type::Value::GetDataFromStorage(type_id, field_location);
      // Call the corresponding varlen pool free
      if (varlen_ptr != nullptr) {
        tile->pool->Free(varlen_ptr);
      }
    }
  }
  }

}
}
