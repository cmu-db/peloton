//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compressed_tile.cpp
//
// Identification: src/storage/compressed_tile.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <sstream>

#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/serializer.h"
#include "type/types.h"
#include "type/ephemeral_pool.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/storage_manager.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"
#include "storage/tuple_iterator.h"

namespace peloton {
namespace storage {

CompressedTile::Tile(BackendType backend_type, TileGroupHeader *tile_header,
           const catalog::Schema &tuple_schema, TileGroup *tile_group,
           int tuple_count)
    : database_id(INVALID_OID),
      table_id(INVALID_OID),
      tile_group_id(INVALID_OID),
      tile_id(INVALID_OID),
      backend_type(backend_type),
      schema(tuple_schema),
      data(NULL),
      tile_group(tile_group),
      pool(NULL),
      num_tuple_slots(tuple_count),
      column_count(tuple_schema.GetColumnCount()),
      tuple_length(tuple_schema.GetLength()),
      uninlined_data_size(0),
      column_header(NULL),
      column_header_size(INVALID_OID),
      tile_group_header(tile_header) {
  PL_ASSERT(tuple_count > 0);

  tile_size = tuple_count * tuple_length;
  // LOG_INFO("Tuple Count: %d for Tile ID: %d and Tile Group ID: %d",tuple_count, tile_id, tile_group_id);

  // allocate tuple storage space for inlined data
  auto &storage_manager = storage::StorageManager::GetInstance();
  data = reinterpret_cast<char *>(
      storage_manager.Allocate(backend_type, tile_size));
  PL_ASSERT(data != NULL);

  // zero out the data
  PL_MEMSET(data, 0, tile_size);

  // allocate pool for blob storage if schema not inlined
  // if (schema.IsInlined() == false) {
  pool = new type::EphemeralPool();
  //}
}

Tile::~Tile() {
  // reclaim the tile memory (INLINED data)
  auto &storage_manager = storage::StorageManager::GetInstance();
  storage_manager.Release(backend_type, data);
  data = NULL;

  // reclaim the tile memory (UNINLINED data)
  // if (schema.IsInlined() == false) {
  delete pool;
  //}
  pool = NULL;

  // clear any cached column headers
  if (column_header) delete column_header;
  column_header = NULL;
}