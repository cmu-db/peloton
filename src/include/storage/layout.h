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

class Layout {
 public:

  Layout (const oid_t num_columns);

  Layout(const column_map_type& column_map);

  Layout(const column_map_type& column_map, oid_t layout_id);

  // Check whether the Layout is a RowStore.
  inline bool IsRowStore() const { return (layout_id_ == ROW_STORE_OID); }

  oid_t  GetLayoutId() const { return layout_id_; }

  // Sets the tile id and column id w.r.t that tile corresponding to
  // the specified tile group column id.
  void LocateTileAndColumn(oid_t column_offset, oid_t &tile_offset,
                           oid_t &tile_column_offset) const;

  double GetLayoutDifference(const storage::Layout &other) const;

  oid_t GetTileIdFromColumnId(oid_t column_id) const;

  oid_t GetTileColumnId(oid_t column_id) const;

  oid_t GetColumnCount() const { return num_columns_;}

  std::string GetMapInfo() const;

 private:

  // Layout Id of the tile
  oid_t layout_id_;

  // Number of columns in the layout 
  oid_t num_columns_; 

  // Layout of the columns.
  column_map_type column_layout_;

};


} //namespace storage
} //namespace peloton