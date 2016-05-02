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
    std::this_thread::sleep_for(
        std::chrono::milliseconds(GC_PERIOD_MILLISECONDS));

    LOG_INFO("Unlink tuple thread...");

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
void GCManager::Reclaim(const cid_t &max_cid) {
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

      // Remove from the original map
      garbage = reclaim_map_.erase(garbage);
      tuple_counter++;
    } else {
      // Early break since we use an ordered map
      break;
    }
  }
  LOG_INFO("Marked %d tuples as recycled", tuple_counter);
}

void GCManager::Unlink(const cid_t &max_cid) {
  int tuple_counter = 0;

  // we check if any possible garbage is actually garbage
  // every time we garbage collect at most MAX_ATTEMPT_COUNT tuples.
  for (size_t i = 0; i < MAX_ATTEMPT_COUNT; ++i) {
    TupleMetadata tuple_metadata;
    // if there's no more tuples in the queue, then break.
    if (unlink_queue_.TryPop(tuple_metadata) == false) {
      break;
    }

    if (tuple_metadata.tuple_end_cid < max_cid) {
      // Now that we know we need to recycle tuple, we need to delete all
      // tuples from the indexes to which it belongs as well.
      DeleteTupleFromIndexes(tuple_metadata);

      // Add to the garbage map
      reclaim_map_.insert(std::make_pair(max_cid, tuple_metadata));
      tuple_counter++;

    } else {
      // if a tuple cannot be reclaimed, then add it back to the list.
      unlink_queue_.BlockingPush(tuple_metadata);
    }
  }  // end for

  LOG_INFO("Marked %d tuples as garbage", tuple_counter);
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

  // FIXME: what if the list is full?
  unlink_queue_.BlockingPush(tuple_metadata);
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

// delete a tuple from all its indexes it belongs to.
void GCManager::DeleteTupleFromIndexes(const TupleMetadata &tuple_metadata) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tuple_metadata.tile_group_id);
  LOG_INFO("Deleting index for tuple(%u, %u)", tuple_metadata.tile_group_id,
           tuple_metadata.tuple_slot_id);

  assert(tile_group != nullptr);
  storage::DataTable *table =
      dynamic_cast<storage::DataTable *>(tile_group->GetAbstractTable());
  assert(table != nullptr);

  // construct the expired version.
  std::unique_ptr<storage::Tuple> expired_tuple(
      new storage::Tuple(table->GetSchema(), true));
  tile_group->CopyTuple(tuple_metadata.tuple_slot_id, expired_tuple.get());

  // unlink the version from all the indexes.
  for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
    auto index = table->GetIndex(idx);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    // build key.
    std::unique_ptr<storage::Tuple> key(
        new storage::Tuple(table->GetSchema(), true));
    key->SetFromTuple(expired_tuple.get(), indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY: {
        LOG_INFO("Deleting primary index");
        // find next version the index bucket should point to.
        auto tile_group_header = tile_group->GetHeader();
        ItemPointer next_version =
            tile_group_header->GetNextItemPointer(tuple_metadata.tuple_slot_id);
        // do we need to reset the prev_item_pointer for next_version??
        assert(next_version.IsNull() == false);

        std::vector<ItemPointerContainer *> item_pointer_containers;
        // find the bucket.
        index->ScanKey(key.get(), item_pointer_containers);
        // as this is primary key, there should be exactly one entry.
        assert(item_pointer_containers.size() == 1);

        // the end_cid of last version is equal to the begin_cid of current
        // version.
        item_pointer_containers[0]
            ->SwapItemPointer(next_version, tuple_metadata.tuple_end_cid);

      } break;
      default: {
        LOG_INFO("Deleting other index");
        index->DeleteEntry(key.get(),
                           ItemPointer(tuple_metadata.tile_group_id,
                                       tuple_metadata.tuple_slot_id));
      }
    }
  }
}

}  // namespace gc
}  // namespace peloton
