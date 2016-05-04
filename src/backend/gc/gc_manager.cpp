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

#include "backend/common/types.h"
#include "backend/gc/gc_manager.h"
#include "backend/index/index.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include <list>

namespace peloton {
namespace gc {

GCBuffer::~GCBuffer(){
  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();
  // Add all garbage tuples to GC manager
  LOG_INFO("Register garbage");
  if(garbage_tuples.size() != 0) {
    cid_t garbage_timestamp = transaction_manager.GetNextCommitId();
    for (auto garbage : garbage_tuples) {
      gc::GCManagerFactory::GetInstance().RecycleTupleSlot(
        table_id, garbage.block, garbage.offset, garbage_timestamp);
    }
  }
}

void GCManager::StartGC() {
  LOG_INFO("Starting GC");
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  is_running_ = true;
  gc_thread_.reset(new std::thread(&GCManager::Running, this));
}

void GCManager::StopGC() {
  LOG_INFO("Stopping GC");
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  this->is_running_ = false;
  this->gc_thread_->join();
  ClearGarbage();
}

// Return false if the tuple's table (tile group) is dropped.
// In such case, this recycled tuple can not be added to the recycled_list.
// Since no one will use it any more, keeping track of it is useless.
// Note that, if we drop a single tile group without dropping the whole table,
// such assumption is problematic.
bool GCManager::ResetTuple(const TupleMetadata &tuple_metadata) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tuple_metadata.tile_group_id);

  // During the resetting, a table may deconstruct because of the DROP TABLE request
  if (tile_group == nullptr) {
    LOG_INFO("Garbage tuple(%u, %u) in table %u no longer exists",
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
      storage::TileGroupHeader::GetReserverdSize());
  // TODO: set the unused 2 boolean value
  LOG_INFO("Garbage tuple(%u, %u) in table %u is reset",
           tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
           tuple_metadata.table_id);
  return true;
}

void GCManager::AddToRecycleMap(const TupleMetadata &tuple_metadata) {
  // If the tuple being reset no longer exists, just skip it
  if (ResetTuple(tuple_metadata) == false) return;

  // Add to the recycle map
  std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue;
  // if the entry for table_id exists.
  if (recycle_queue_map_.find(tuple_metadata.table_id, recycle_queue) ==
      true) {
    // if the entry for tuple_metadata.table_id exists.
    recycle_queue->BlockingPush(tuple_metadata);
  } else {
    // if the entry for tuple_metadata.table_id does not exist.
    recycle_queue.reset(
      new LockfreeQueue<TupleMetadata>(MAX_QUEUE_LENGTH));
    bool ret =
      recycle_queue_map_.insert(tuple_metadata.table_id, recycle_queue);
    if (ret == true) {
      recycle_queue->BlockingPush(tuple_metadata);
    } else {
      recycle_queue_map_.find(tuple_metadata.table_id, recycle_queue);
      recycle_queue->BlockingPush(tuple_metadata);
    }
  }
}

void GCManager::Running() {
  // Check if we can move anything from the possibly free list to the free list.

  // We use a local buffer to store all possible garbage handled by this gc worker
  std::list<TupleMetadata> local_reclaim_queue;

  while (true) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(GC_PERIOD_MILLISECONDS));

    LOG_INFO("reclaim tuple thread...");

    // First load every possible garbage into the list
    // This step move all garbage from the global reclaim queue to the worker's local queue
    for (size_t i = 0; i < MAX_ATTEMPT_COUNT; ++i) {
      TupleMetadata tuple_metadata;
      if (reclaim_queue_.TryPop(tuple_metadata) == false) {
        break;
      }
      LOG_INFO("Collect tuple (%u, %u) of table %u into local list",
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
        LOG_INFO("Add tuple(%u, %u) in table %u to recycle map", queue_itr->tile_group_id,
                 queue_itr->tuple_slot_id, queue_itr->table_id);
        AddToRecycleMap(*queue_itr);
        queue_itr = local_reclaim_queue.erase(queue_itr);
        tuple_counter++;
      } else {
        queue_itr++;
      }
      attempt++;
    }

    LOG_INFO("Marked %d tuples as garbage", tuple_counter);
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

      LOG_INFO("GCThread recycle last %d tuples before exits", tuple_counter);
      return;
    }
  }
}

// called by transaction manager.
void GCManager::RecycleTupleSlot(const oid_t &table_id,
                                 const oid_t &tile_group_id,
                                 const oid_t &tuple_id,
                                 const cid_t &tuple_end_cid) {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }

  TupleMetadata tuple_metadata;
  tuple_metadata.table_id = table_id;
  tuple_metadata.tile_group_id = tile_group_id;
  tuple_metadata.tuple_slot_id = tuple_id;
  tuple_metadata.tuple_end_cid = tuple_end_cid;

  reclaim_queue_.BlockingPush(tuple_metadata);

  LOG_INFO("Marked tuple(%u, %u) in table %u as possible garbage",
           tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
           tuple_metadata.table_id);
}

// this function returns a free tuple slot, if one exists
// called by data_table.
ItemPointer GCManager::ReturnFreeSlot(const oid_t &table_id) {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return INVALID_ITEMPOINTER;
  }

  std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue;
  // if there exists recycle_queue
  if (recycle_queue_map_.find(table_id, recycle_queue) == true) {
    TupleMetadata tuple_metadata;
    if (recycle_queue->TryPop(tuple_metadata) == true) {
      LOG_INFO("Reuse tuple(%u, %u) in table %u", tuple_metadata.tile_group_id,
               tuple_metadata.tuple_slot_id, table_id);
      return ItemPointer(tuple_metadata.tile_group_id,
                         tuple_metadata.tuple_slot_id);
    }
  }
  return ItemPointer();
}

// this function can only be called after:
//    1) All txns have exited
//    2) The background gc thread has exited
void GCManager::ClearGarbage() {
  // iterate reclaim queue and reclaim every thing because it's the end of the world now.
  TupleMetadata tuple_metadata;
  int counter = 0;
  while (reclaim_queue_.TryPop(tuple_metadata) == true) {
    // In such case, we assume it's the end of the world and every possible
    // garbage is actually garbage
    AddToRecycleMap(tuple_metadata);
    counter++;
  }

  LOG_INFO("GCManager finally recyle %d tuples", counter);
}

}  // namespace gc
}  // namespace peloton
