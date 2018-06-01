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

#include "optimizer/stats/tuple_sampler.h"
#include <cinttypes>

#include "storage/data_table.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "storage/tuple.h"

namespace peloton {
namespace optimizer {

/**
 * AcquireSampleTuples - Sample a certain number of tuples from a given table.
 * This function performs random sampling by generating random tile_group_offset
 * and random tuple_offset.
 */
size_t TupleSampler::AcquireSampleTuples(size_t target_sample_count) {
  size_t tuple_count = table->GetTupleCount();
  size_t tile_group_count = table->GetTileGroupCount();
  LOG_TRACE("tuple_count = %lu, tile_group_count = %lu", tuple_count,
            tile_group_count);

  if (tuple_count < target_sample_count) {
    target_sample_count = tuple_count;
  }

  size_t rand_tilegroup_offset, rand_tuple_offset;
  srand(time(NULL));
  catalog::Schema *tuple_schema = table->GetSchema();

  while (sampled_tuples.size() < target_sample_count) {
    // Generate a random tilegroup offset
    rand_tilegroup_offset = rand() % tile_group_count;
    storage::TileGroup *tile_group =
        table->GetTileGroup(rand_tilegroup_offset).get();
    oid_t tuple_per_group = tile_group->GetActiveTupleCount();
    LOG_TRACE("tile_group: offset: %lu, addr: %p, tuple_per_group: %u",
              rand_tilegroup_offset, tile_group, tuple_per_group);
    if (tuple_per_group == 0) {
      continue;
    }

    rand_tuple_offset = rand() % tuple_per_group;

    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(tuple_schema, true));

    LOG_TRACE("tuple_group_offset = %lu, tuple_offset = %lu",
              rand_tilegroup_offset, rand_tuple_offset);
    if (!GetTupleInTileGroup(tile_group, rand_tuple_offset, tuple)) {
      continue;
    }
    LOG_TRACE("Add sampled tuple: %s", tuple->GetInfo().c_str());
    sampled_tuples.push_back(std::move(tuple));
  }
  LOG_TRACE("%lu Sample added - size: %lu", sampled_tuples.size(),
            sampled_tuples.size() * tuple_schema->GetLength());
  return sampled_tuples.size();
}

/**
 * GetTupleInTileGroup - This function is a helper function to get a tuple in
 * a tile group.
 */
bool TupleSampler::GetTupleInTileGroup(storage::TileGroup *tile_group,
                                       size_t tuple_offset,
                                       std::unique_ptr<storage::Tuple> &tuple) {
  // Tile Group Header
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

  // Check whether tuple is valid at given offset in the tile_group
  // Reference: TileGroupHeader::GetActiveTupleCount()
  // Check whether the transaction ID is invalid.
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_offset);
  LOG_TRACE("transaction ID: %" PRId64, tuple_txn_id);
  if (tuple_txn_id == INVALID_TXN_ID) {
    return false;
  }

  size_t tuple_column_itr = 0;
  size_t tile_count = tile_group->GetTileCount();

  LOG_TRACE("tile_count: %lu", tile_count);
  for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {

    storage::Tile *tile = tile_group->GetTile(tile_itr);
    const catalog::Schema &schema = *(tile->GetSchema());
    uint32_t tile_column_count = schema.GetColumnCount();

    char *tile_tuple_location = tile->GetTupleLocation(tuple_offset);
    storage::Tuple tile_tuple(&schema, tile_tuple_location);

    for (oid_t tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      type::Value val = (tile_tuple.GetValue(tile_column_itr));
      tuple->SetValue(tuple_column_itr, val, pool_.get());
      tuple_column_itr++;
    }
  }
  LOG_TRACE("offset %lu, Tuple info: %s", tuple_offset,
            tuple->GetInfo().c_str());

  return true;
}

size_t TupleSampler::AcquireSampleTuplesForIndexJoin(
    std::vector<std::unique_ptr<storage::Tuple>> &sample_tuples,
    std::vector<std::vector<ItemPointer *>> &matched_tuples, size_t count) {
  size_t target = std::min(count, sample_tuples.size());
  std::vector<size_t> sid;
  for (size_t i = 1; i <= target; i++) {
    sid.push_back(i);
  }
  srand(time(NULL));
  for (size_t i = target + 1; i <= count; i++) {
    if (rand() % i < target) {
      size_t pos = rand() % target;
      sid[pos] = i;
    }
  }
  for (auto id : sid) {
    size_t chosen = 0;
    size_t cnt = 0;
    while (cnt < id) {
      cnt += matched_tuples.at(chosen).size();
      if (cnt >= id) {
        break;
      }
      chosen++;
    }

    size_t offset = rand() % matched_tuples.at(chosen).size();
    auto item = matched_tuples.at(chosen).at(offset);
    storage::TileGroup *tile_group = table->GetTileGroupById(item->block).get();

    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(table->GetSchema(), true));
    GetTupleInTileGroup(tile_group, item->offset, tuple);
    LOG_TRACE("tuple info %s", tuple->GetInfo().c_str());
    AddJoinTuple(sample_tuples.at(chosen), tuple);
  }
  LOG_TRACE("join schema info %s",
            sampled_tuples[0]->GetSchema()->GetInfo().c_str());
  return sampled_tuples.size();
}

void TupleSampler::AddJoinTuple(std::unique_ptr<storage::Tuple> &left_tuple,
                                std::unique_ptr<storage::Tuple> &right_tuple) {
  if (join_schema == nullptr) {
    std::unique_ptr<catalog::Schema> left_schema(
        catalog::Schema::CopySchema(left_tuple->GetSchema()));
    std::unique_ptr<catalog::Schema> right_schema(
        catalog::Schema::CopySchema(right_tuple->GetSchema()));
    join_schema.reset(
        catalog::Schema::AppendSchema(left_schema.get(), right_schema.get()));
  }
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(join_schema.get(), true));
  for (oid_t i = 0; i < left_tuple->GetColumnCount(); i++) {
    tuple->SetValue(i, left_tuple->GetValue(i), pool_.get());
  }

  oid_t column_offset = left_tuple->GetColumnCount();
  for (oid_t i = 0; i < right_tuple->GetColumnCount(); i++) {
    tuple->SetValue(i + column_offset, right_tuple->GetValue(i), pool_.get());
  }
  LOG_TRACE("join tuple info %s", tuple->GetInfo().c_str());

  sampled_tuples.push_back(std::move(tuple));
}

/**
 * GetSampledTuples - This function returns the sampled tuples.
 */
std::vector<std::unique_ptr<storage::Tuple>> &TupleSampler::GetSampledTuples() {
  return sampled_tuples;
}

}  // namespace optimizer
}  // namespace peloton
