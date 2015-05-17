/*-------------------------------------------------------------------------
 *
 * context.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/context.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "common/context.h"
#include "storage/tile_group.h"

namespace nstore {

bool Context::Commit(){

  auto& manager = catalog::Manager::GetInstance();

  // inserted slots
  for(auto slot : inserted_slots){
    storage::TileGroup *tile_group = static_cast<storage::TileGroup *>(slot.location);
    assert(tile_group);

    bool status = tile_group->CommitInsertedTuple(slot.offset, local_commit_id);
    if(status == false)
      return false;
  }

  // deleted slots
  for(auto slot : deleted_slots){
    storage::TileGroup *tile_group = static_cast<storage::TileGroup *>(slot.location);
    assert(tile_group);

    bool status = tile_group->CommitDeletedTuple(slot.offset, txn_id, local_commit_id);
    if(status == false)
      return false;
  }

  return true;
}

bool Context::Abort(){

  auto& manager = catalog::Manager::GetInstance();

  // inserted slots
  for(auto slot : inserted_slots){
    storage::TileGroup *tile_group = static_cast<storage::TileGroup *>(slot.location);
    assert(tile_group);

    bool status = tile_group->AbortInsertedTuple(slot.offset);
    if(status == false)
      return false;
  }

  // deleted slots
  for(auto slot : deleted_slots){
    storage::TileGroup *tile_group = static_cast<storage::TileGroup *>(slot.location);
    assert(tile_group);

    bool status = tile_group->AbortDeletedTuple(slot.offset);
    if(status == false)
      return false;
  }

  return true;
}

} // End nstore namespace


