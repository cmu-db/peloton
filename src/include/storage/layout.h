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
#include "common/printable.h"

#define ROW_STORE_OID 0
#define COLUMN_STORE_OID 1

namespace peloton {

namespace catalog {
class Schema;
}

namespace storage {

class Layout : public Printable {
 public:

  Layout (const oid_t num_columns, LayoutType layout_type = LayoutType::ROW);

  Layout(const column_map_type& column_map);

  Layout(const column_map_type& column_map, oid_t layout_id);

  // Check whether the Layout is a RowStore.
  inline bool IsRowStore() const { return (layout_type_ == LayoutType::ROW); }

  // Check whether the Layout is ColumnStore.
  inline bool IsColumnStore() const { return (layout_type_ == LayoutType::COLUMN); }

  oid_t  GetOid() const { return layout_oid_; }

  // Sets the tile id and column id w.r.t that tile corresponding to
  // the specified tile group column id.
  void LocateTileAndColumn(oid_t column_offset, oid_t &tile_offset,
                           oid_t &tile_column_offset) const;

  double GetLayoutDifference(const storage::Layout &other) const;

  oid_t GetTileIdFromColumnId(oid_t column_id) const;

  oid_t GetTileColumnId(oid_t column_id) const;

  oid_t GetColumnCount() const { return num_columns_;}

  std::vector<catalog::Schema> GetLayoutSchemas(catalog::Schema* const schema) const;

  std::map<oid_t, oid_t> GetColumnLayoutStats() const;

  std::string SerializeColumnMap() const;

  static column_map_type DeserializeColumnMap(oid_t num_columns,
                                              std::string column_map_str);

  std::string GetColumnMapInfo() const;

  // Get a string representation for debugging
  const std::string GetInfo() const;

  friend bool operator==(const Layout& lhs, const Layout& rhs);
  friend bool operator!=(const Layout& lhs, const Layout& rhs);

 private:

  // Layout Id of the tile
  oid_t layout_oid_;

  // Number of columns in the layout 
  oid_t num_columns_; 

  // Layout of the columns.
  column_map_type column_layout_;

  LayoutType layout_type_;

};

} //namespace storage
} //namespace peloton