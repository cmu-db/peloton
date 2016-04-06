/*-------------------------------------------------------------------------
 *
 * checkpoint.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/checkpoint.h"
#include "backend/index/index.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
std::string Checkpoint::ConcatFileName(std::string checkpoint_dir,
                                       int version) {
  return checkpoint_dir + "/" + FILE_PREFIX + std::to_string(version) +
         FILE_SUFFIX;
}

void Checkpoint::InitDirectory() {
  int return_val;

  return_val = mkdir(checkpoint_dir.c_str(), 0700);
  LOG_INFO("Checkpoint directory is: %s", checkpoint_dir.c_str());

  if (return_val == 0) {
    LOG_INFO("Created checkpoint directory successfully");
  } else if (errno == EEXIST) {
    LOG_INFO("Checkpoint Directory already exists");
  } else {
    LOG_ERROR("Creating checkpoint directory failed: %s", strerror(errno));
  }
}

void Checkpoint::RecoverIndex(storage::Tuple *tuple, storage::DataTable *table,
                              ItemPointer target_location) {
  assert(tuple);
  assert(table);
  auto index_count = table->GetIndexCount();
  LOG_TRACE("Insert tuple (%lu, %lu) into %lu indexes", target_location.block,
            target_location.offset, index_count);

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = table->GetIndex(index_itr);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    index->InsertEntry(key.get(), target_location);
    // Increase the indexes' number of tuples by 1 as well
    index->IncreaseNumberOfTuplesBy(1);
  }
}

void Checkpoint::RecoverTuple(storage::Tuple *tuple, storage::DataTable *table,
                              ItemPointer target_location, cid_t commit_id) {
  auto tile_group_id = target_location.block;
  auto tuple_slot = target_location.offset;

  LOG_TRACE("Recover tuple from checkpoint (%lu, %lu)", tile_group_id,
            tuple_slot);

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);

  // Create new tile group if table doesn't already have that tile group
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(tile_group_id);
    tile_group = manager.GetTileGroup(tile_group_id);
  }

  // Do the insert!
  auto inserted_tuple_slot =
      tile_group->InsertTupleFromCheckpoint(tuple_slot, tuple, commit_id);

  if (inserted_tuple_slot == INVALID_OID) {
    // TODO: We need to abort on failure!
  } else {
    // TODO this is not thread safe
    table->SetNumberOfTuples(table->GetNumberOfTuples() + 1);
  }
}

}  // namespace logging
}  // namespace peloton
