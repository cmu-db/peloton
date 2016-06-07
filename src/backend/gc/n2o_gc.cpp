//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// n2o_gc.cpp
//
// Identification: src/backend/gc/n2o_gc.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/gc/n2o_gc.h"
#include "backend/index/index.h"
#include "backend/concurrency/transaction_manager_factory.h"
namespace peloton {
namespace gc {

void N2O_GCManager::StartGC() {
  this->is_running_ = true;
  gc_thread_.reset(new std::thread(&N2O_GCManager::Running, this));
}

void N2O_GCManager::StopGC() {
  this->is_running_ = false;
  this->gc_thread_->join();
  ClearGarbage();
}

bool N2O_GCManager::ResetTuple(const TupleMetadata &tuple_metadata) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group =
    manager.GetTileGroup(tuple_metadata.tile_group_id);

  // During the resetting, a table may deconstruct because of the DROP TABLE request
  if (tile_group == nullptr) {
    LOG_TRACE("Garbage tuple(%u, %u) in table %u no longer exists",
             tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
             tuple_metadata.table_id);
    return false;
  }

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
  return true;
}

void N2O_GCManager::Running() {
  // Check if we can move anything from the possibly free list to the free list.

  while (true) {
    std::this_thread::sleep_for(
      std::chrono::milliseconds(GC_PERIOD_MILLISECONDS));

    LOG_TRACE("Unlink tuple thread...");

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto max_cid = txn_manager.GetMaxCommittedCid();

    assert(max_cid != MAX_CID);

    Reclaim(max_cid);

    Unlink(max_cid);

    if (is_running_ == false) {
      return;
    }
  }
}

// executed by a single thread. so no synchronization is required.
void N2O_GCManager::Reclaim(const cid_t &max_cid) {
  int tuple_counter = 0;

  // we delete garbage in the free list
  auto garbage = reclaim_map_.begin();
  while (garbage != reclaim_map_.end()) {
    const cid_t garbage_ts = garbage->first;
    const TupleMetadata &tuple_metadata = garbage->second;

    // if the timestamp of the garbage is older than the current max_cid,
    // recycle it
    if (garbage_ts < max_cid) {
      ResetTuple(tuple_metadata);

      assert(recycle_queue_map_.find(tuple_metadata.table_id) != recycle_queue_map_.end());
      recycle_queue_map_[tuple_metadata.table_id]->Enqueue(tuple_metadata);

      // Add to the recycle map
      //std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue;

      // if the entry for table_id exists.
      // if (recycle_queue_map_.find(tuple_metadata.table_id, recycle_queue) == true) {
      //   // if the entry for tuple_metadata.table_id exists.
      //   recycle_queue->Enqueue(tuple_metadata);
      // } else {
      //   // if the entry for tuple_metadata.table_id does not exist.
      //   recycle_queue.reset(new LockfreeQueue<TupleMetadata>(MAX_QUEUE_LENGTH));
      //   recycle_queue->Enqueue(tuple_metadata);
      //   recycle_queue_map_[tuple_metadata.table_id] = recycle_queue;
      // }

      // Remove from the original map
      garbage = reclaim_map_.erase(garbage);
      tuple_counter++;
    } else {
      // Early break since we use an ordered map
      break;
    }
  }
  LOG_TRACE("Marked %d tuples as recycled", tuple_counter);
}

void N2O_GCManager::Unlink(const cid_t &max_cid) {
  int tuple_counter = 0;

  // we check if any possible garbage is actually garbage
  // every time we garbage collect at most MAX_ATTEMPT_COUNT tuples.

  std::vector<TupleMetadata> garbages;
  for (size_t i = 0; i < MAX_ATTEMPT_COUNT; ++i) {
    TupleMetadata tuple_metadata;
    // if there's no more tuples in the queue, then break.
    if (unlink_queue_.Dequeue(tuple_metadata) == false) {
      break;
    }

    if (tuple_metadata.tuple_end_cid < max_cid) {
      // Now that we know we need to recycle tuple, we need to delete all
      // tuples from the indexes to which it belongs as well.
      //DeleteTupleFromIndexes(tuple_metadata);

      // Add to the garbage map
      // reclaim_map_.insert(std::make_pair(getnextcid(), tuple_metadata));
      garbages.push_back(tuple_metadata);
      tuple_counter++;

    } else {
      // if a tuple cannot be reclaimed, then add it back to the list.
      unlink_queue_.Enqueue(tuple_metadata);
    }
  }  // end for

  auto safe_max_cid = concurrency::TransactionManagerFactory::GetInstance().GetNextCommitId();
  for(auto& item : garbages){
    reclaim_map_.insert(std::make_pair(safe_max_cid, item));
  }
  LOG_TRACE("Marked %d tuples as garbage", tuple_counter);
}

// called by transaction manager.
void N2O_GCManager::RecycleOldTupleSlot(const oid_t &table_id,
                                 const oid_t &tile_group_id,
                                 const oid_t &tuple_id,
                                 const cid_t &tuple_end_cid) {

  TupleMetadata tuple_metadata;
  tuple_metadata.table_id = table_id;
  tuple_metadata.tile_group_id = tile_group_id;
  tuple_metadata.tuple_slot_id = tuple_id;
  tuple_metadata.tuple_end_cid = tuple_end_cid;

  // FIXME: what if the list is full?
  unlink_queue_.Enqueue(tuple_metadata);
  LOG_TRACE("Marked tuple(%u, %u) in table %u as possible garbage",
           tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
           tuple_metadata.table_id);
}

void N2O_GCManager::RecycleInvalidTupleSlot(const oid_t &table_id __attribute__((unused)),
                                           const oid_t &tile_group_id __attribute__((unused)),
                                           const oid_t &tuple_id __attribute__((unused))) {
  assert(false);
}


// this function returns a free tuple slot, if one exists
// called by data_table.
ItemPointer N2O_GCManager::ReturnFreeSlot(const oid_t &table_id) {
  // return INVALID_ITEMPOINTER;
  assert(recycle_queue_map_.count(table_id) != 0);
  TupleMetadata tuple_metadata;
  auto recycle_queue = recycle_queue_map_[table_id];


  //std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue;
  // if there exists recycle_queue
  //if (recycle_queue_map_.find(table_id, recycle_queue) == true) {
    //TupleMetadata tuple_metadata;
    if (recycle_queue->Dequeue(tuple_metadata) == true) {
      LOG_TRACE("Reuse tuple(%u, %u) in table %u", tuple_metadata.tile_group_id,
               tuple_metadata.tuple_slot_id, table_id);
      return ItemPointer(tuple_metadata.tile_group_id,
                         tuple_metadata.tuple_slot_id);
    }
  //}
  return INVALID_ITEMPOINTER;
}


void N2O_GCManager::ClearGarbage() {
  while(!unlink_queue_.IsEmpty()) {
    Unlink(MAX_CID);
  }

  while(reclaim_map_.size() != 0) {
    Reclaim(MAX_CID);
  }

  return;
}

}  // namespace gc
}  // namespace peloton
