//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_catalog.cpp
//
// Identification: src/
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/zone_map_catalog.h"

#include "catalog/catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "type/types.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

// Global Singleton : I really dont want to do this and I know this sucks.
// but #796 is still not merged when I am writing this code and I really 
// am sorry to do this. When PelotonMain() becomes a reality, I will 
// fix this for sure.
ZoneMapCatalog *ZoneMapCatalog::GetInstance(
    concurrency::Transaction *txn) {
  static ZoneMapCatalog zone_map_catalog{txn};
  return &zone_map_catalog;
}

ZoneMapCatalog::ZoneMapCatalog(concurrency::Transaction *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." ZONE_MAP_CATALOG_NAME
                      " ("
                      "database_id    INT NOT NULL, "
                      "table_id       INT NOT NULL, "
                      "tile_group_id  INT NOT NULL,  "
                      "column_id      INT NOT NULL, "
                      "minimum        VARCHAR, "
                      "maximum        VARCHAR, "
                      "type           VARCHAR);",
                      txn) {

  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, ZONE_MAP_CATALOG_NAME, {0,1,2,3},
      ZONE_MAP_CATALOG_NAME "_skey0", true, IndexType::BWTREE, txn);
}

ZoneMapCatalog::~ZoneMapCatalog() {}

bool ZoneMapCatalog::InsertColumnStatistics(
    oid_t database_id, oid_t table_id, oid_t tile_group_id, oid_t column_id,
    std::string minimum, std::string maximum, std::string type, type::AbstractPool *pool,
    concurrency::Transaction *txn) {

  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val_db_id = type::ValueFactory::GetIntegerValue(database_id);
  auto val_table_id = type::ValueFactory::GetIntegerValue(table_id);
  auto val_tile_group_id = type::ValueFactory::GetIntegerValue(tile_group_id);
  auto val_column_id = type::ValueFactory::GetIntegerValue(column_id);
  auto val_minimum = type::ValueFactory::GetVarcharValue(minimum);
  auto val_maximum = type::ValueFactory::GetVarcharValue(maximum);
  auto val_type = type::ValueFactory::GetVarcharValue(type);

  tuple->SetValue(ColumnId::DATABASE_ID, val_db_id, nullptr);
  tuple->SetValue(ColumnId::TABLE_ID, val_table_id, nullptr);
  tuple->SetValue(ColumnId::TILE_GROUP_ID, val_tile_group_id, nullptr);
  tuple->SetValue(ColumnId::COLUMN_ID, val_column_id, nullptr);
  tuple->SetValue(ColumnId::MINIMUM, val_minimum, pool);
  tuple->SetValue(ColumnId::MAXIMUM, val_maximum, pool);
  tuple->SetValue(ColumnId::TYPE, val_type, pool);

  bool return_val = InsertTuple(std::move(tuple), txn);
  LOG_DEBUG("Inserted Tuple : %d", return_val);
  return return_val;
}

bool ZoneMapCatalog::DeleteColumnStatistics(oid_t database_id, oid_t table_id,
                                           oid_t tile_group_id, oid_t column_id,
                                           concurrency::Transaction *txn) {
  oid_t index_offset = IndexId::SECONDARY_KEY_0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(tile_group_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_id).Copy());
  return DeleteWithIndexScan(index_offset, values, txn);
}

std::unique_ptr<std::vector<type::Value>> ZoneMapCatalog::GetColumnStatistics(
    oid_t database_id, oid_t table_id, oid_t tile_group_id, oid_t column_id,
    concurrency::Transaction *txn) {
  LOG_DEBUG("Database Id : %u , Table Id : %u , TileGroup Id : %u, Column Id : %u ", database_id, table_id, tile_group_id, column_id);
  std::vector<oid_t> column_ids(
      {ColumnId::MINIMUM, ColumnId::MAXIMUM, ColumnId::TYPE});

  oid_t index_offset = IndexId::SECONDARY_KEY_0;

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(tile_group_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_id).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  PL_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() == 0) {
    LOG_DEBUG("Result Tiles = 0");
    return nullptr;
  }

  auto tile = (*result_tiles)[0].get();
  PL_ASSERT(tile->GetTupleCount() <= 1);
  if (tile->GetTupleCount() == 0) {
    LOG_DEBUG("Result Tile has tuple count = 0");
    return nullptr;
  }

  type::Value min, max, actual_type;
  min = tile->GetValue(0, ZoneMapOffset::MINIMUM_OFF);
  max = tile->GetValue(0, ZoneMapOffset::MAXIMUM_OFF);
  actual_type = tile->GetValue(0, ZoneMapOffset::TYPE_OFF);

  // min and max are stored as VARCHARs and should be convertd to their 
  // original types.

  std::unique_ptr<std::vector<type::Value>> zone_map(
      new std::vector<type::Value>({min, max, actual_type}));

  return zone_map;
}

}  // namespace catalog
}  // namespace peloton
