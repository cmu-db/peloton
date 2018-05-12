//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// layout.cpp
//
// Identification: src/storage/layout.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <string>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "storage/layout.h"
#include "util/stringbox_util.h"

namespace peloton {
namespace storage {

// Constructor for the layout class with column_count
// The default layout is always a ROW_STORE
Layout::Layout(const oid_t num_columns, LayoutType layout_type)
    : num_columns_(num_columns), layout_type_(layout_type) {}

// Constructor for the Layout class with column_map
Layout::Layout(const column_map_type &column_map)
    : num_columns_(column_map.size()), column_layout_(column_map) {
  // Figure out the layout type if not present
  bool row_layout = true, column_layout = true;
  for (oid_t column_id = 0; column_id < num_columns_; column_id++) {
    if (column_layout_[column_id].first != 0) {
      row_layout = false;
    }
    if (column_layout_[column_id].first != column_id) {
      column_layout = false;
    }
  }

  // Assign the layout_type_
  // If a table has one column, it would be LayoutType::Row
  if (row_layout) {
    layout_type_ = LayoutType::ROW;
    layout_oid_ = ROW_STORE_OID;
  } else if (column_layout) {
    layout_type_ = LayoutType::COLUMN;
    layout_oid_ = COLUMN_STORE_OID;
  } else {
    // layout_oid_ is set to INVALID_OID, indicating that this
    // layout is not stored in the catalog and thus not persistent.
    // To be used only in TempTable or Tests.
    layout_type_ = LayoutType::HYBRID;
    layout_oid_ = INVALID_OID;
  }

  if (layout_type_ != LayoutType::HYBRID) {
    // Since this is a predefined layout, we don't need the map
    column_layout_.clear();
  }
}

// Constructor for Layout class with predefined layout_oid
Layout::Layout(const column_map_type &column_map, oid_t layout_id)
    : layout_oid_(layout_id),
      num_columns_(column_map.size()),
      column_layout_(column_map) {
  if (layout_oid_ == ROW_STORE_OID) {
    layout_type_ = LayoutType::ROW;
  } else if (layout_oid_ == COLUMN_STORE_OID) {
    layout_type_ = LayoutType::COLUMN;
  } else {
    layout_type_ = LayoutType::HYBRID;
  }

  if (layout_type_ != LayoutType::HYBRID) {
    // Since this is a predefined layout, we don't need the map
    column_layout_.clear();
  }
}

void Layout::LocateTileAndColumn(oid_t column_id, oid_t &tile_id,
                                 oid_t &tile_column_id) const {
  // Ensure that the column_offset is not out of bound
  PELOTON_ASSERT(num_columns_ > column_id);

  // For row store layout, tile_id is always 0 and the
  // column_id and tile column_id are the same.
  if (layout_type_ == LayoutType::ROW) {
    tile_id = 0;
    tile_column_id = column_id;
    return;
  }

  // For column store layout, tile_id is same as column_id
  // and the tile_column_id is always 0.
  if (layout_type_ == LayoutType::COLUMN) {
    tile_id = column_id;
    tile_column_id = 0;
    return;
  }

  // For other layouts, fetch the layout and
  // get the entry in the column map
  auto entry = column_layout_.at(column_id);
  tile_id = entry.first;
  tile_column_id = entry.second;
}

double Layout::GetLayoutDifference(const storage::Layout &other) const {
  double theta = 0;
  double diff = 0;

  // Calculate theta only for TileGroups with the same schema.
  // Check the schema before invoking this function.
  PELOTON_ASSERT(this->num_columns_ == other.num_columns_);

  if ((this->layout_oid_ != other.layout_oid_)) {
    for (oid_t col_itr = 0; col_itr < num_columns_; col_itr++) {
      if (this->GetTileIdFromColumnId(col_itr) !=
          other.GetTileIdFromColumnId(col_itr)) {
        diff++;
      }
    }
    // compute theta
    theta = diff / num_columns_;
  }

  return theta;
}

oid_t Layout::GetTileIdFromColumnId(oid_t column_id) const {
  oid_t tile_column_id, tile_offset;
  LocateTileAndColumn(column_id, tile_offset, tile_column_id);
  return tile_offset;
}

TileToColumnMap Layout::GetTileMap() const {
  TileToColumnMap tile_map;

  if (layout_type_ == LayoutType::ROW) {
    // Row store layout, hence all columns are contained in tile 0.
    // The column_offset is always the same as column_id.
    tile_map[0] = {};
    for (oid_t column_id = 0; column_id < num_columns_; column_id++) {
      tile_map[0].emplace_back(column_id, column_id);
    }
  } else if (layout_type_ == LayoutType::COLUMN) {
    // Column store layout, hence all columns are contained in separate tiles.
    // The column_offset within the tile is always 0.
    for (oid_t column_id = 0; column_id < num_columns_; column_id++) {
      tile_map[column_id] = {std::make_pair(column_id, 0)};
    }
  } else {
    // Hybrid layout, hence figure out the mapping from the column_layout_.
    for (auto entry : column_layout_) {
      auto column_id = entry.first;
      auto tile_id = entry.second.first;
      auto column_offset = entry.second.second;
      if (tile_map.find(tile_id) == tile_map.end()) {
        tile_map[tile_id] = {};
      }
      tile_map[tile_id].emplace_back(column_id, column_offset);
    }
  }
  return tile_map;
}

oid_t Layout::GetTileColumnOffset(oid_t column_id) const {
  oid_t tile_column_id, tile_offset;
  LocateTileAndColumn(column_id, tile_offset, tile_column_id);
  return tile_column_id;
}

std::vector<catalog::Schema> Layout::GetLayoutSchemas(
    catalog::Schema *const schema) const {
  std::vector<catalog::Schema> schemas;

  // Build the schema tile at a time
  std::map<oid_t, std::vector<catalog::Column>> tile_schemas;

  // Handle schemas population based on the layout type
  if (layout_type_ == LayoutType::ROW) {
    schemas.push_back(*schema);
  } else if (layout_type_ == LayoutType::COLUMN) {
    for (oid_t col_id = 0; col_id < num_columns_; col_id++) {
      std::vector<catalog::Column> tile_schema({schema->GetColumn(col_id)});
      schemas.push_back(tile_schema);
    }
  } else {
    // For LayoutType::HYBRID, use the old technique.
    // This only works if the order of columns in the tile are the same
    // as the schema. This snipet was initially in abstract_table.cpp.
    for (auto column_info : column_layout_) {
      tile_schemas[column_info.second.first].push_back(
          schema->GetColumn(column_info.first));
    }

    for (auto entry : tile_schemas) {
      catalog::Schema tile_schema(entry.second);
      schemas.push_back(tile_schema);
    }
  }

  return schemas;
}

std::map<oid_t, oid_t> Layout::GetLayoutStats() const {
  std::map<oid_t, oid_t> column_map_stats;

  // Cluster per-tile column count
  for (auto entry : column_layout_) {
    auto tile_id = entry.second.first;
    auto column_map_itr = column_map_stats.find(tile_id);
    if (column_map_itr == column_map_stats.end())
      column_map_stats[tile_id] = 1;
    else
      column_map_stats[tile_id]++;
  }

  return column_map_stats;
}

std::string Layout::SerializeColumnMap() const {
  std::stringstream ss;

  for (auto column_info : column_layout_) {
    ss << column_info.first << ":";
    ss << column_info.second.first << ":";
    ss << column_info.second.second << ",";
  }

  return ss.str();
}

column_map_type Layout::DeserializeColumnMap(oid_t num_columns,
                                             std::string column_map_str) {
  column_map_type column_map;
  std::stringstream ss(column_map_str);

  for (oid_t col_id = 0; col_id < num_columns; col_id++) {
    oid_t str_col_id, tile_id, tile_col_id;
    // Read col_id from column_map_str
    ss >> str_col_id;
    PELOTON_ASSERT(str_col_id == col_id);

    PELOTON_ASSERT(ss.peek() == ':');
    ss.ignore();
    // Read tile_id from column_map_str
    ss >> tile_id;

    PELOTON_ASSERT(ss.peek() == ':');
    ss.ignore();
    // Read tile_col_id from column_map_str
    ss >> tile_col_id;

    // Insert the column info into column_map
    column_map[col_id] = std::make_pair(tile_id, tile_col_id);

    if (ss.peek() == ',') {
      ss.ignore();
    }
  }

  return column_map;
}

std::string Layout::GetColumnMapInfo() const {
  std::stringstream ss;
  std::map<oid_t, std::vector<oid_t>> tile_column_map;

  if (layout_type_ == LayoutType::ROW) {
    // Row store always contains only 1 tile. The tile_id is always 0.
    oid_t tile_id = 0;
    tile_column_map[tile_id] = {};
    for (oid_t col_id = 0; col_id < num_columns_; col_id++) {
      tile_column_map[tile_id].push_back(col_id);
    }
  } else if (layout_type_ == LayoutType::COLUMN) {
    // Column store always contains 1 column per tile.
    oid_t tile_col_id = 0;
    for (oid_t col_id = 0; col_id < num_columns_; col_id++) {
      tile_column_map[col_id].push_back(tile_col_id);
    }
  } else {
    for (auto column_info : column_layout_) {
      oid_t col_id = column_info.first;
      oid_t tile_id = column_info.second.first;
      if (tile_column_map.find(tile_id) == tile_column_map.end()) {
        tile_column_map[tile_id] = {};
      }
      tile_column_map[tile_id].push_back(col_id);
    }
  }

  // Construct a string from tile_col_map
  for (auto tile_info : tile_column_map) {
    ss << tile_info.first << " : ";
    for (auto col_id : tile_info.second) {
      ss << col_id << " ";
    }
    ss << " :: ";
  }

  return ss.str();
}

const std::string Layout::GetInfo() const {
  std::ostringstream os;

  os << peloton::GETINFO_DOUBLE_STAR << " Layout[#" << layout_oid_ << "] "
     << peloton::GETINFO_DOUBLE_STAR << std::endl;
  os << "Number of columns[" << num_columns_ << "] " << std::endl;
  os << "LayoutType[" << LayoutTypeToString(layout_type_) << std::endl;
  os << "ColumnMap[ " << GetColumnMapInfo() << "] " << std::endl;

  return peloton::StringUtil::Prefix(peloton::StringBoxUtil::Box(os.str()),
                                     GETINFO_SPACER);
}

bool operator==(const Layout &lhs, const Layout &rhs) {
  // Check the equality of layout_type_
  if (lhs.layout_type_ != rhs.layout_type_) {
    return false;
  }

  // Check the equality of layout_oid_
  if (lhs.GetOid() != rhs.GetOid()) {
    return false;
  }
  // Check the equality of column_count_
  if (lhs.GetColumnCount() != rhs.GetColumnCount()) {
    return false;
  }

  // Check for the equality of the column_layout_ for
  // LayoutType::HYBRID
  if (lhs.layout_type_ == LayoutType::HYBRID) {
    return (lhs.column_layout_ == rhs.column_layout_);
  }
  // In case of LayoutType::ROW or LayoutType::COLUMN,
  // there is no need to check for column_layout_ equality

  return true;
}

bool operator!=(const Layout &lhs, const Layout &rhs) { return !(lhs == rhs); }

}  // namespace storage
}  // namespace peloton