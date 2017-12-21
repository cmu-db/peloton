//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_table.cpp
//
// Identification: src/storage/abstract_table.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/abstract_table.h"

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "index/index.h"
#include "storage/tile_group.h"
#include "storage/tile_group_factory.h"
#include "util/stringbox_util.h"

namespace peloton {
namespace storage {

AbstractTable::AbstractTable(oid_t table_oid, catalog::Schema *schema,
                             bool own_schema, peloton::LayoutType layout_type)
    : table_oid(table_oid), schema(schema), own_schema_(own_schema), layout_type(layout_type) {}

AbstractTable::~AbstractTable() {
  // clean up schema
  if (own_schema_) delete schema;
}

column_map_type AbstractTable::GetTileGroupLayout() const {
  column_map_type column_map;

  auto col_count = schema->GetColumnCount();

  // pure row layout map
  if (layout_type == LayoutType::ROW) {
    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      column_map[col_itr] = std::make_pair(0, col_itr);
    }
  }
  // pure column layout map
  else if (layout_type == LayoutType::COLUMN) {
    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      column_map[col_itr] = std::make_pair(col_itr, 0);
    }
  }
  // hybrid layout map
  else if (layout_type == LayoutType::HYBRID) {
    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      column_map[col_itr] = std::make_pair(0, col_itr);
    }
  } else {
    throw Exception("Unknown tilegroup layout option : " +
                    LayoutTypeToString(layout_type));
  }

  return column_map;
}

TileGroup *AbstractTable::GetTileGroupWithLayout(
    oid_t database_id, oid_t tile_group_id, const column_map_type &partitioning,
    const size_t num_tuples) {
  std::vector<catalog::Schema> schemas;

  // Figure out the columns in each tile in new layout
  std::map<std::pair<oid_t, oid_t>, oid_t> tile_column_map;
  for (auto entry : partitioning) {
    tile_column_map[entry.second] = entry.first;
  }

  // Build the schema tile at a time
  std::map<oid_t, std::vector<catalog::Column>> tile_schemas;
  for (auto entry : tile_column_map) {
    tile_schemas[entry.first.first].push_back(schema->GetColumn(entry.second));
  }
  for (auto entry : tile_schemas) {
    catalog::Schema tile_schema(entry.second);
    schemas.push_back(tile_schema);
  }

  TileGroup *tile_group =
      TileGroupFactory::GetTileGroup(database_id, GetOid(), tile_group_id, this,
                                     schemas, partitioning, num_tuples);

  return tile_group;
}

const std::string AbstractTable::GetInfo() const {
  std::ostringstream inner;
  oid_t tile_group_count = this->GetTileGroupCount();
  oid_t tuple_count = 0;
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    if (tile_group_itr > 0) inner << std::endl;

    auto tile_group = this->GetTileGroup(tile_group_itr);
    auto tile_tuple_count = tile_group->GetNextTupleSlot();

    std::string tileData = tile_group->GetInfo();

    inner << tileData;
    tuple_count += tile_tuple_count;
  }

  std::ostringstream output;
  output << "Table '" << GetName() << "' [";
  output << "OID= " << GetOid() << ", ";
  output << "NumTuples=" << tuple_count << ", ";
  output << "NumTiles=" << tile_group_count << "]" << std::endl;
  output << inner.str();

  return output.str();
}

}  // namespace storage
}  // namespace peloton
