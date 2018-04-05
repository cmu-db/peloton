//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple.cpp
//
// Identification: src/storage/layout.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <sstream>

#include "storage/layout.h"
#include "util/stringbox_util.h"

namespace peloton {
namespace storage {

// Constructor for the layout class with column_count
Layout::Layout(const oid_t num_columns)
        : layout_id_(ROW_STORE_OID),
          num_columns_(num_columns) {}

// Constructor for the Layout class with column_map
Layout::Layout(const column_map_type& column_map)
        : layout_id_(ROW_STORE_OID),
          num_columns_(column_map.size()),
          column_layout_(column_map) {
  for (oid_t column_id = 0; column_id < num_columns_; column_id++) {
    if (column_layout_[column_id].first != 0) {
      layout_id_ = INVALID_OID;
      return;
    }
  }
  // Since this is a row store, we don't need the map
  column_layout_.clear();
}

// Constructor for Layout class with predefined layout_id
Layout::Layout(const column_map_type &column_map, oid_t layout_id)
        : layout_id_(layout_id),
          num_columns_(column_map.size()),
          column_layout_(column_map) {}


// Sets the tile id and column id w.r.t that tile corresponding to
// the specified tile group column id.
void Layout::LocateTileAndColumn(oid_t column_offset,
    oid_t &tile_offset, oid_t &tile_column_offset) const {

	// Ensure that the column_offset is not out of bound
	PELOTON_ASSERT(num_columns_ > column_offset);

	// For row store layout, tile id is always 0 and the tile
	// column_id and tile_group column_id is the same.
	if (layout_id_ == ROW_STORE_OID) {
		tile_offset = 0;
		tile_column_offset = column_offset;
		return;
	}

	// For other layouts, fetch the layout and
	// get the entry in the column map
	auto entry = column_layout_.at(column_offset);
	tile_offset = entry.first;
	tile_column_offset = entry.second;
}

double Layout::GetLayoutDifference(const storage::Layout &other) const {
  double theta = 0;
  double diff = 0;

  // Calculate theta only for TileGroups with the same schema.
  // TODO Pooja: Handle the case where two Layouts with same
  // number of columns have a different schema.
  PELOTON_ASSERT(this->num_columns_ == other.num_columns_);

  if ((this->layout_id_ != other.layout_id_)) {

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

oid_t Layout::GetTileColumnId(oid_t column_id) const {
  oid_t tile_column_id, tile_offset;
  LocateTileAndColumn(column_id, tile_offset, tile_column_id);
  return tile_column_id;
}

std::string Layout::GetColumnMapInfo() const {
  std::stringstream ss;
  std::map<oid_t, std::vector<oid_t>> tile_column_map;

  if (layout_id_ == ROW_STORE_OID) {
    // Row store always contains only 1 tile. The tile_id is always 0.
    oid_t tile_id = 0;
    tile_column_map[tile_id] = {};
    for (oid_t col_id = 0; col_id < num_columns_; col_id++) {
      tile_column_map[tile_id].push_back(col_id);
    }
  } else {
    for (auto column_info : column_layout_) {
      oid_t col_id = column_info.first;
      oid_t tile_id = column_info.second.first;
      if(tile_column_map.find(tile_id) == tile_column_map.end()) {
        tile_column_map[tile_id] = {};
      }
      tile_column_map[tile_id].push_back(col_id);
    }
  }

  // Construct a string from tile_col_map
  for (auto tile_info : tile_column_map) {
    ss << tile_info.first << ": ";
    for (auto col_id : tile_info.second) {
      ss << col_id << " ";
    }
    ss << " :: ";
  }

  return ss.str();
}

const std::string Layout::GetInfo() const {
  std::ostringstream os;

  os << peloton::GETINFO_DOUBLE_STAR << " Layout[#" << layout_id_ << "] "
     << peloton::GETINFO_DOUBLE_STAR << std::endl;
  os << "Number of columns[" << num_columns_ << "] " << std::endl;

  os << "ColumnMap[ " << GetColumnMapInfo() << "] " << std::endl;

  return peloton::StringUtil::Prefix(peloton::StringBoxUtil::Box(os.str()),
                                     GETINFO_SPACER);
}


} // namespace storage
} // namespace peloton