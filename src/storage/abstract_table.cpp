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
    : table_oid(table_oid), schema(schema), own_schema_(own_schema) {
  // The default Layout should always be ROW or COLUMN
  PELOTON_ASSERT((layout_type == LayoutType::ROW) ||
                 (layout_type == LayoutType::COLUMN));
  default_layout_ = std::shared_ptr<const Layout>(
      new Layout(schema->GetColumnCount(), layout_type));
}

AbstractTable::~AbstractTable() {
  // clean up schema
  if (own_schema_) delete schema;
}

TileGroup *AbstractTable::GetTileGroupWithLayout(
    oid_t database_id, oid_t tile_group_id,
    std::shared_ptr<const Layout> layout, const size_t num_tuples) {
  // Populate the schema for each tile
  std::vector<catalog::Schema> schemas = layout->GetLayoutSchemas(schema);

  TileGroup *tile_group = TileGroupFactory::GetTileGroup(
      database_id, GetOid(), tile_group_id, this, schemas, layout, num_tuples);

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
