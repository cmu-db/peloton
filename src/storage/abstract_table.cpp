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
#include "gc/gc_manager_factory.h"
#include "index/index.h"
#include "storage/tile_group.h"
#include "util/stringbox_util.h"

namespace peloton {
namespace storage {

AbstractTable::AbstractTable(id_t table_oid, catalog::Schema *schema,
                             bool own_schema)
    : table_oid(table_oid), schema(schema), own_schema_(own_schema) {
  // Register table to GC manager.
  auto *gc_manager = &gc::GCManagerFactory::GetInstance();
  assert(gc_manager != nullptr);
  gc_manager->RegisterTable(table_oid);
}

AbstractTable::~AbstractTable() {
  // clean up schema
  if (own_schema_) delete schema;
}

column_map_type AbstractTable::GetTileGroupLayout(
    LayoutType layout_type) const {
  column_map_type column_map;

  auto col_count = schema->GetColumnCount();

  // pure row layout map
  if (layout_type == LAYOUT_TYPE_ROW) {
    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      column_map[col_itr] = std::make_pair(0, col_itr);
    }
  }
  // pure column layout map
  else if (layout_type == LAYOUT_TYPE_COLUMN) {
    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      column_map[col_itr] = std::make_pair(col_itr, 0);
    }
  }
  // hybrid layout map
  else if (layout_type == LAYOUT_TYPE_HYBRID) {
    for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
      column_map[col_itr] = std::make_pair(0, col_itr);
    }
  } else {
    throw Exception("Unknown tilegroup layout option : " +
                    std::to_string(layout_type));
  }

  return column_map;
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
    //    dataBuffer << tileData;
    inner << peloton::StringUtil::Prefix(peloton::StringBoxUtil::Box(tileData),
                                         GETINFO_SPACER);
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

}  // End storage namespace
}  // End peloton namespace
