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
namespace peloton {
namespace gc {

void GCManager::StartGC() {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  gc_thread_.reset(new std::thread(&GCManager::Running, this));
}

void GCManager::StopGC() {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  this->is_running_ = false;
  this->gc_thread_->join();
}

void GCManager::ResetTuple(const TupleMetadata &tuple_metadata) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header =
      manager.GetTileGroup(tuple_metadata.tile_group_id)->GetHeader();

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
}

void GCManager::Running() {
  // Check if we can move anything from the possibly free list to the free list.

  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(GC_PERIOD_MILLISECONDS));

    LOG_INFO("reclaim tuple thread...");

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto max_cid = txn_manager.GetMaxCommittedCid();

    assert(max_cid != MAX_CID);

    int tuple_counter = 0;

    // every time we garbage collect at most MAX_ATTEMPT_COUNT tuples.
    for (size_t i = 0; i < MAX_ATTEMPT_COUNT; ++i) {
      TupleMetadata tuple_metadata;
      // if there's no more tuples in the queue, then break.
      if (reclaim_queue_.TryPop(tuple_metadata) == false) {
        break;
      }

      if (tuple_metadata.tuple_end_cid < max_cid) {
        ResetTuple(tuple_metadata);

        // Add to the recycle map
        std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue;
        // if the entry for table_id exists.
        if (recycle_queue_map_.find(tuple_metadata.table_id, recycle_queue) == true) {
          // if the entry for tuple_metadata.table_id exists.
          recycle_queue->BlockingPush(tuple_metadata);
        } else {
          // if the entry for tuple_metadata.table_id does not exist.
          recycle_queue.reset(new LockfreeQueue<TupleMetadata>(MAX_QUEUE_LENGTH));
          recycle_queue->BlockingPush(tuple_metadata);
          recycle_queue_map_[tuple_metadata.table_id] = recycle_queue;
        }

        tuple_counter++;
      } else {
        // if a tuple cannot be reclaimed, then add it back to the list.
        reclaim_queue_.BlockingPush(tuple_metadata);
      }
    }  // end for

    LOG_INFO("Marked %d tuples as garbage", tuple_counter);

    if (is_running_ == false) {
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
            tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id, tuple_metadata.table_id);
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
       LOG_INFO("Reuse tuple(%u, %u) in table %u",
                 tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id, table_id);
       return ItemPointer(tuple_metadata.tile_group_id,
                          tuple_metadata.tuple_slot_id);
     }
   }
  return ItemPointer();
}

}  // namespace gc
}  // namespace peloton
