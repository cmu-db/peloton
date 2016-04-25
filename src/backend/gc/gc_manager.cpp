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
  gc_thread_.reset(new std::thread(&GCManager::Reclaim, this));
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

void GCManager::Reclaim() {
  // Check if we can move anything from the possibly free list to the free list.

  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(GC_PERIOD_MILLISECONDS));

    LOG_INFO("reclaim tuple thread...");

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto max_cid = txn_manager.GetMaxCommittedCid();

    assert(max_cid != MAX_CID);

    int tuple_counter = 0;

    // we delete garbage in the free list
    // auto garbage = garbage_map_.begin();
    // while (garbage != garbage_map_.end()) {
    //   const cid_t garbage_ts = garbage->first;
    //   const TupleMetadata &tuple_metadata = garbage->second;

    //   // if the timestamp of the garbage is older than the current max_cid, recycle it
    //   if (garbage_ts < max_cid) {
    //     ResetTuple(tuple_metadata);

    //     // Add to the recycle map
    //     std::shared_ptr<LockfreeQueue<TupleMetadata>> free_list;
    //     // if the entry for table_id exists.
    //     if (recycled_map_.find(tuple_metadata.table_id, free_list) == true) {
    //       // if the entry for tuple_metadata.table_id exists.
    //       free_list->Push(tuple_metadata);
    //     } else {
    //       // if the entry for tuple_metadata.table_id does not exist.
    //       free_list.reset(new LockfreeQueue<TupleMetadata>(MAX_TUPLES_PER_GC));
    //       free_list->Push(tuple_metadata);
    //       recycled_map_[tuple_metadata.table_id] = free_list;
    //     }

    //     // Remove from the original map

    //     garbage = garbage_map_.erase(garbage);
    //     tuple_counter++;
    //   } else {
    //     // Early break since we use an ordered map
    //     break;
    //   }
    // }
    // LOG_INFO("Marked %d tuples as recycled", tuple_counter);
    //tuple_counter = 0;

    // Next, we check if any possible garbage is actually garbage
    // every time we garbage collect at most MAX_TUPLES_PER_GC tuples.
    for (size_t i = 0; i < MAX_TUPLES_PER_GC; ++i) {
      TupleMetadata tuple_metadata;
      // if there's no more tuples in the queue, then break.
      if (garbage_list_.Pop(tuple_metadata) == false) {
        break;
      }

      if (tuple_metadata.tuple_end_cid < max_cid) {
        ResetTuple(tuple_metadata);

        // Add to the recycle map
        std::shared_ptr<LockfreeQueue<TupleMetadata>> free_list;
        // if the entry for table_id exists.
        if (recycled_map_.find(tuple_metadata.table_id, free_list) == true) {
          // if the entry for tuple_metadata.table_id exists.
          free_list->Push(tuple_metadata);
        } else {
          // if the entry for tuple_metadata.table_id does not exist.
          free_list.reset(new LockfreeQueue<TupleMetadata>(MAX_TUPLES_PER_GC));
          free_list->Push(tuple_metadata);
          recycled_map_[tuple_metadata.table_id] = free_list;
        }
        // Now that we know we need to recycle tuple, we need to delete all
        // tuples from the indexes to which it belongs as well.
        //DeleteTupleFromIndexes(tuple_metadata);

        // Add to the garbage map
        // garbage_map_.insert(garbage_map_.find(max_cid),
        //                    std::make_pair(max_cid, tuple_metadata));
        tuple_counter++;
      } else {
        // if a tuple cannot be reclaimed, then add it back to the list.
        garbage_list_.Push(tuple_metadata);
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

  // FIXME: what if the list is full?
  //possibly_free_list_.Push(tuple_metadata);
  //garbage_map_lock_.Lock();
  garbage_list_.Push(tuple_metadata);
  //garbage_map_lock_.Unlock();

  LOG_INFO("Marked tuple(%u, %u) in table %u as possible garbage",
            tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id, tuple_metadata.table_id);
}

// this function returns a free tuple slot, if one exists
// called by data_table.
ItemPointer GCManager::ReturnFreeSlot(const oid_t &table_id) {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return INVALID_ITEMPOINTER;
  }

   std::shared_ptr<LockfreeQueue<TupleMetadata>> free_list;
   // if there exists free_list
   if (recycled_map_.find(table_id, free_list) == true) {
     TupleMetadata tuple_metadata;
     if (free_list->Pop(tuple_metadata) == true) {
       LOG_INFO("Reuse tuple(%u, %u) in table %u",
                 tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id, table_id);
       return ItemPointer(tuple_metadata.tile_group_id,
                          tuple_metadata.tuple_slot_id);
     }
   }
  return ItemPointer();
}

// delete a tuple from all its indexes it belongs to.
// void GCManager::DeleteTupleFromIndexes(const TupleMetadata &tuple_metadata) {
//   auto &manager = catalog::Manager::GetInstance();
//   auto tile_group = manager.GetTileGroup(tuple_metadata.tile_group_id);
//   LOG_INFO("Deleting index for tuple(%u, %u)", tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id);

//   assert(tile_group != nullptr);
//   storage::DataTable *table =
//       dynamic_cast<storage::DataTable *>(tile_group->GetAbstractTable());
//   assert(table != nullptr);

//   // construct the expired version.
//   std::unique_ptr<storage::Tuple> expired_tuple(
//       new storage::Tuple(table->GetSchema(), true));
//   tile_group->CopyTuple(tuple_metadata.tuple_slot_id, expired_tuple.get());

//   // unlink the version from all the indexes.
//   for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
//     auto index = table->GetIndex(idx);
//     auto index_schema = index->GetKeySchema();
//     auto indexed_columns = index_schema->GetIndexedColumns();

//     // build key.
//     std::unique_ptr<storage::Tuple> key(
//         new storage::Tuple(table->GetSchema(), true));
//     key->SetFromTuple(expired_tuple.get(), indexed_columns, index->GetPool());

//     switch (index->GetIndexType()) {
//       case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY: {
//         LOG_INFO("Deleting primary index");
//         // find next version the index bucket should point to.
//         auto tile_group_header = tile_group->GetHeader();
//         ItemPointer next_version =
//             tile_group_header->GetNextItemPointer(tuple_metadata.tuple_slot_id);
//         // do we need to reset the prev_item_pointer for next_version??
//         assert(next_version.IsNull() == false);

//         std::vector<ItemPointerContainer *> item_pointer_containers;
//         // find the bucket.
//         index->ScanKey(key.get(), item_pointer_containers);
//         // as this is primary key, there should be exactly one entry.
//         assert(item_pointer_containers.size() == 1);

//         // the end_cid of last version is equal to the begin_cid of current
//         // version.
//         item_pointer_containers[0]->SwapItemPointer(
//             next_version, tuple_metadata.tuple_end_cid);

//       } break;
//       default: {
//         LOG_INFO("Deleting other index");
//         index->DeleteEntry(key.get(),
//                            ItemPointer(tuple_metadata.tile_group_id,
//                                        tuple_metadata.tuple_slot_id));
//       }
//     }
//   }
// }

}  // namespace gc
}  // namespace peloton
