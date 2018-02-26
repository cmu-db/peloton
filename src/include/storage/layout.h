//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple.h
//
// Identification: src/include/storage/layout.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>

#include "common/internal_types.h"

#define ROW_STORE_OID 0

namespace peloton {
namespace storage {

typedef std::map<oid_t, std::pair<oid_t, oid_t>> column_map_type;

class Layout {
 public:

  Layout(const column_map_type& column_map);

  // Check whether the Layout is a RowStore.
  inline bool IsRowStore() const { return (layout_id_ == ROW_STORE_OID); }

  oid_t  GetLayoutId() const { return layout_id_; }

  void LocateTileAndColumn(oid_t column_offset, oid_t &tile_offset,
                           oid_t &tile_column_offset) const;

  oid_t GetTileIdFromColumnId(oid_t column_id);

  oid_t GetTileColumnId(oid_t column_id);

 private:

  // Layout Id of the tile
  oid_t layout_id_;

  // Number of columns in the layout 
  oid_t num_columns_; 

  // <column offset> to <tile offset, tile column offset>
  column_map_type tile_group_layout_;

};


} //namespace storage
} //namespace peloton