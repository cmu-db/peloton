//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_sampler.cpp
//
// Identification: src/optimizer/tuple_sampler.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/tuple_sampler.h"

#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "storage/tile.h"
#include "storage/tuple.h"

namespace peloton {
namespace optimizer {

void TupleSampler::InitState() {

}

bool TupleSampler::HasNext() {
    return false;
}

int TupleSampler::GetNext() {
    return 0;
}

/**
 * AcquireSampleTuples - Sample a certain number of tuples from a given table.
 *
 */
size_t TupleSampler::AcquireSampleTuples(size_t target_sample_count, size_t *total_tuple_count) {
//  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();

//  auto &manager = catalog::Manager::GetInstance();

  size_t tuple_count = table->GetTupleCount();
  size_t tile_group_count = table->GetTileGroupCount();
  LOG_DEBUG("tuple_count = %lu, tile_group_count = %lu",
              tuple_count, tile_group_count);

  *total_tuple_count = tuple_count;

  if(tuple_count < target_sample_count) {
    target_sample_count = tuple_count;
  }
  size_t sampled_count = 0;

  size_t rand_tilegroup_offset, rand_tuple_offset;
  srand(time(NULL));
  catalog::Schema *tuple_schema = table->GetSchema();

  while(sampled_count < target_sample_count) {
    // Generate a random tilegroup offset
    rand_tilegroup_offset = rand() % tile_group_count;
    storage::TileGroup *tile_group = table->GetTileGroup(rand_tilegroup_offset).get();
    oid_t tuple_per_group = tile_group->GetActiveTupleCount();
    LOG_DEBUG("tile_group: offset: %lu, addr: %p, tuple_per_group: %u",
        rand_tilegroup_offset, tile_group, tuple_per_group);
    if(tuple_per_group == 0) {
      continue;
    }

    rand_tuple_offset = rand() % tuple_per_group;

    storage::Tuple tuple(tuple_schema, true);

    LOG_DEBUG("tuple_group_offset = %lu, tuple_offset = %lu",
                rand_tilegroup_offset, rand_tuple_offset);
    if(!GetTupleInTileGroup(tile_group, rand_tuple_offset, &tuple)) {
      continue;
    }

    sampled_tuples.push_back(tuple);
    sampled_count ++;
  }
  return sampled_tuples.size();
}

bool TupleSampler::GetTupleInTileGroup(storage::TileGroup *tile_group,
                          size_t tuple_offset,
                          storage::Tuple *tuple) {
  // Tile Group Header
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
//  LOG_DEBUG("Tile Group info: %s", tile_group_header->GetInfo().c_str());

  // Check whether tuple is valid at given offset in the tile_group
  // Reference: TileGroupHeader::GetActiveTupleCount()
  // Check whether the transaction ID is invalid.
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_offset);
  LOG_DEBUG("transaction ID: %lu", tuple_txn_id);
  if (tuple_txn_id == INVALID_TXN_ID) {
    return false;
  }

  size_t tuple_column_itr = 0;
  auto tile_schemas = tile_group->GetTileSchemas();
  size_t tile_count = tile_group->GetTileCount();

  LOG_DEBUG("tile_count: %lu", tile_count);
  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    const catalog::Schema &schema = tile_schemas[tile_itr];
    oid_t tile_column_count = schema.GetColumnCount();

    storage::Tile *tile = tile_group->GetTile(tile_itr);

    char *tile_tuple_location = tile->GetTupleLocation(tuple_offset);
    storage::Tuple tile_tuple(&schema, tile_tuple_location);

    for (oid_t tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      type::Value val = (tile_tuple.GetValue(tile_column_itr));
      tuple->SetValue(tuple_column_itr, val);
      tuple_column_itr++;
    }
  }
  LOG_DEBUG("offset %lu, Tuple info: %s", tuple_offset, tuple->GetInfo().c_str());
  return true;
}


} /* namespace optimizer */
} /* namespace peloton */
