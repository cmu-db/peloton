//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager.cpp
//
// Identification: src/backend/gc/gc_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/gc/cooperative_gc.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include <list>

namespace peloton {
namespace gc {

GCBuffer::~GCBuffer(){
  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();
  // Add all garbage tuples to GC manager
  LOG_TRACE("Register garbage");
  if(garbage_tuples.size() != 0) {
    cid_t garbage_timestamp = transaction_manager.GetNextCommitId();
    for (auto garbage : garbage_tuples) {
      gc::GCManagerFactory::GetInstance().RecycleOldTupleSlot(
        table_id, garbage.block, garbage.offset, garbage_timestamp);
    }
  }
}


void Cooperative_GCManager::StartGC() {
  LOG_TRACE("Starting GC");
  this->is_running_ = true;
  gc_thread_.reset(new std::thread(&Cooperative_GCManager::Running, this));
}

void Cooperative_GCManager::StopGC() {
  LOG_TRACE("Stopping GC");
  this->is_running_ = false;
  this->gc_thread_->join();
  ClearGarbage();
}

// Return false if the tuple's table (tile group) is dropped.
// In such case, this recycled tuple can not be added to the recycled_list.
// Since no one will use it any more, keeping track of it is useless.
// Note that, if we drop a single tile group without dropping the whole table,
// such assumption is problematic.
bool Cooperative_GCManager::ResetTuple(const TupleMetadata &tuple_metadata) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tuple_metadata.tile_group_id);

  // During the resetting, a table may deconstruct because of the DROP TABLE request
  if (tile_group == nullptr) {
    LOG_TRACE("Garbage tuple(%u, %u) in table %u no longer exists",
             tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
             tuple_metadata.table_id);
    return false;
  }

  // From now on, the tile group shared pointer is held by us
  // It's safe to set headers from now on.

  auto tile_group_header = tile_group->GetHeader();

  // Reset the header
  tile_group_header->SetTransactionId(tuple_metadata.tuple_slot_id,
                                      INVALID_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_metadata.tuple_slot_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_metadata.tuple_slot_id, MAX_CID);
  tile_group_header->SetPrevItemPointer(tuple_metadata.tuple_slot_id,
                                        INVALID_ITEMPOINTER);
  tile_group_header->SetNextItemPointer(tuple_metadata.tuple_slot_id,
                                        INVALID_ITEMPOINTER);
  std::memset(
    tile_group_header->GetReservedFieldRef(tuple_metadata.tuple_slot_id), 0,
    storage::TileGroupHeader::GetReservedSize());
  // TODO: set the unused 2 boolean value
  LOG_TRACE("Garbage tuple(%u, %u) in table %u is reset",
           tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
           tuple_metadata.table_id);
  return true;
}

void Cooperative_GCManager::AddToRecycleMap(const TupleMetadata &tuple_metadata) {
  // If the tuple being reset no longer exists, just skip it
  if (ResetTuple(tuple_metadata) == false) return;

  //recycle_queue_.Enqueue(tuple_metadata);

  // Add to the recycle map
  std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue;
  // if the entry for table_id exists.

  assert(recycle_queue_map_.count(tuple_metadata.table_id != 0));
  recycle_queue_map_[tuple_metadata.table_id]->Enqueue(tuple_metadata);
}

void Cooperative_GCManager::Running() {
  // Check if we can move anything from the possibly free list to the free list.

  // We use a local buffer to store all possible garbage handled by this gc worker
  std::list<TupleMetadata> local_reclaim_queue;

  while (true) {
    std::this_thread::sleep_for(
      std::chrono::milliseconds(GC_PERIOD_MILLISECONDS));

    LOG_TRACE("reclaim tuple thread...");

    // First load every possible garbage into the list
    // This step move all garbage from the global reclaim queue to the worker's local queue
    for (size_t i = 0; i < MAX_ATTEMPT_COUNT; ++i) {
      TupleMetadata tuple_metadata;
      if (reclaim_queue_.Dequeue(tuple_metadata) == false) {
        break;
      }
      LOG_TRACE("Collect tuple (%u, %u) of table %u into local list",
               tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id, tuple_metadata.table_id);
      local_reclaim_queue.push_back(tuple_metadata);
    }

    // Then we go through to recycle garbage
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto max_cid = txn_manager.GetMaxCommittedCid();

    assert(max_cid != MAX_CID);

    int tuple_counter = 0;
    int attempt = 0;
    auto queue_itr = local_reclaim_queue.begin();

    while (queue_itr != local_reclaim_queue.end() && attempt < MAX_ATTEMPT_COUNT) {
      if (queue_itr->tuple_end_cid <= max_cid) {
        // add the tuple to recycle map
        LOG_TRACE("Add tuple(%u, %u) in table %u to recycle map", queue_itr->tile_group_id,
                 queue_itr->tuple_slot_id, queue_itr->table_id);
        AddToRecycleMap(*queue_itr);
        queue_itr = local_reclaim_queue.erase(queue_itr);
        tuple_counter++;
      } else {
        queue_itr++;
      }
      attempt++;
    }

    LOG_TRACE("Marked %d tuples as garbage", tuple_counter);
    if (is_running_ == false) {
      // Clear all pending garbage
      tuple_counter = 0;
      queue_itr = local_reclaim_queue.begin();

      while (queue_itr != local_reclaim_queue.end()) {
        // In this case, we assume that no transaction is running
        // so every possible garbage is actually garbage
        AddToRecycleMap(*queue_itr);
        queue_itr = local_reclaim_queue.erase(queue_itr);
        tuple_counter ++;
      }

      LOG_TRACE("GCThread recycle last %d tuples before exits", tuple_counter);
      return;
    }
  }
}

void Cooperative_GCManager::RecycleOldTupleSlot(const oid_t &table_id,
                                 const oid_t &tile_group_id,
                                 const oid_t &tuple_id,
                                 const cid_t &tuple_end_cid) {
  TupleMetadata tuple_metadata;
  tuple_metadata.table_id = table_id;
  tuple_metadata.tile_group_id = tile_group_id;
  tuple_metadata.tuple_slot_id = tuple_id;
  tuple_metadata.tuple_end_cid = tuple_end_cid;

  reclaim_queue_.Enqueue(tuple_metadata);

  LOG_TRACE("Marked tuple(%u, %u) in table %u as possible garbage",
           tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
           tuple_metadata.table_id);
}


void Cooperative_GCManager::RecycleInvalidTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                              const oid_t &tuple_id){

  TupleMetadata tuple_metadata;
  tuple_metadata.table_id = table_id;
  tuple_metadata.tile_group_id = tile_group_id;
  tuple_metadata.tuple_slot_id = tuple_id;
  tuple_metadata.tuple_end_cid = START_CID;

  DeleteInvalidTupleFromIndex(tuple_metadata);

  AddToRecycleMap(tuple_metadata);
}


// this function returns a free tuple slot, if one exists
// called by data_table.
ItemPointer Cooperative_GCManager::ReturnFreeSlot(const oid_t &table_id) {
  // if there exists recycle_queue
  assert(recycle_queue_map_.count(table_id) != 0);
  TupleMetadata tuple_metadata;
  auto recycle_queue = recycle_queue_map_[table_id];

  if (recycle_queue->Dequeue(tuple_metadata) == true) {
    LOG_TRACE("Reuse tuple(%u, %u) in table %u", tuple_metadata.tile_group_id,
             tuple_metadata.tuple_slot_id, table_id);
    return ItemPointer(tuple_metadata.tile_group_id,
                       tuple_metadata.tuple_slot_id);
  }
  return ItemPointer();
}


// this function can only be called after:
//    1) All txns have exited
//    2) The background gc thread has exited
void Cooperative_GCManager::ClearGarbage() {
  // iterate reclaim queue and reclaim every thing because it's the end of the world now.
  TupleMetadata tuple_metadata;
  int counter = 0;
  while (reclaim_queue_.Dequeue(tuple_metadata) == true) {
    // In such case, we assume it's the end of the world and every possible
    // garbage is actually garbage
    AddToRecycleMap(tuple_metadata);
    counter++;
  }

  LOG_TRACE("Cooperative_GCManager finally recyle %d tuples", counter);
}

}  // namespace gc
}  // namespace peloton
