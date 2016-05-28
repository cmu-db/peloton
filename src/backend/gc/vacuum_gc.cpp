//
// Created by Zrs_y on 5/10/16.
//

#include "backend/gc/vacuum_gc.h"
#include "backend/index/index.h"
#include "backend/concurrency/transaction_manager_factory.h"
namespace peloton {
namespace gc {

void Vacuum_GCManager::StartGC() {
  this->is_running_ = true;
  gc_thread_.reset(new std::thread(&Vacuum_GCManager::Running, this));
}

void Vacuum_GCManager::StopGC() {
  this->is_running_ = false;
  this->gc_thread_->join();
  ClearGarbage();
}

bool Vacuum_GCManager::ResetTuple(const TupleMetadata &tuple_metadata) {
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
  // TODO: set the unused 2 boolean value
  return true;
}

void Vacuum_GCManager::Running() {
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
void Vacuum_GCManager::Reclaim(const cid_t &max_cid) {
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

void Vacuum_GCManager::Unlink(const cid_t &max_cid) {
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
      DeleteTupleFromIndexes(tuple_metadata);

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
void Vacuum_GCManager::RecycleOldTupleSlot(const oid_t &table_id,
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

void Vacuum_GCManager::RecycleInvalidTupleSlot(const oid_t &table_id,
                                           const oid_t &tile_group_id,
                                           const oid_t &tuple_id) {

  TupleMetadata tuple_metadata;
  tuple_metadata.table_id = table_id;
  tuple_metadata.tile_group_id = tile_group_id;
  tuple_metadata.tuple_slot_id = tuple_id;
  tuple_metadata.tuple_end_cid = START_CID;

  // DeleteInvalidTupleFromIndex(tuple_metadata);
  // ResetTuple(tuple_metadata);

  // assert(recycle_queue_map_.count(table_id) != 0);
  // recycle_queue_map_[table_id]->Enqueue(tuple_metadata);


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

  LOG_TRACE("Marked tuple(%u, %u) in table %u as possible garbage",
           tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id,
           tuple_metadata.table_id);

}


// this function returns a free tuple slot, if one exists
// called by data_table.
ItemPointer Vacuum_GCManager::ReturnFreeSlot(const oid_t &table_id) {

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

// delete a tuple from all its indexes it belongs to.
void Vacuum_GCManager::DeleteTupleFromIndexes(const TupleMetadata &tuple_metadata) {
  LOG_TRACE("Deleting index for tuple(%u, %u)", tuple_metadata.tile_group_id,
           tuple_metadata.tuple_slot_id);

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tuple_metadata.tile_group_id);

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
        LOG_TRACE("Deleting primary index");

        // Do nothing for new to old version chain here
        if (concurrency::TransactionManagerFactory::GetProtocol() == CONCURRENCY_TYPE_OCC_N2O) {
          continue;
        }

        // find next version the index bucket should point to.
        auto tile_group_header = tile_group->GetHeader();
        ItemPointer next_version =
          tile_group_header->GetNextItemPointer(tuple_metadata.tuple_slot_id);

        auto next_tile_group_header = manager.GetTileGroup(next_version.block)->GetHeader();
        auto next_begin_cid = next_tile_group_header->GetBeginCommitId(next_version.offset);
        assert(next_version.IsNull() == false);
        assert(next_begin_cid != MAX_CID);

        std::vector<ItemPointer *> item_pointer_containers;
        // find the bucket.
        index->ScanKey(key.get(), item_pointer_containers);
        // as this is primary key, there should be exactly one entry.
        assert(item_pointer_containers.size() == 1);


        auto index_version = *item_pointer_containers[0];
        auto index_tile_group_header = manager.GetTileGroup(index_version.block)->GetHeader();
        auto index_begin_cid = index_tile_group_header->GetBeginCommitId(index_version.offset);

        // if next_version is newer than index's version
        // update index
        if(index_begin_cid < next_begin_cid) {
          AtomicUpdateItemPointer(item_pointer_containers[0], next_version);
        }

      } break;
      default: {
        LOG_TRACE("Deleting other index");
        index->DeleteEntry(key.get(),
                           ItemPointer(tuple_metadata.tile_group_id,
                                       tuple_metadata.tuple_slot_id));
      }
    }
  }
}

void Vacuum_GCManager::ClearGarbage() {
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
