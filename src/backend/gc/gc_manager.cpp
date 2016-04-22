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
  gc_thread_.reset(new std::thread(&GCManager::Poll, this));
}

void GCManager::StopGC() {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  this->is_running_ = false;
  this->gc_thread_->join();
}

void GCManager::Poll() {
  // Check if we can move anything from the possibly free list to the free list.
  auto &manager = catalog::Manager::GetInstance();

  while (true) {
    LOG_DEBUG("Polling GC thread...");

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto max_cid = txn_manager.GetMaxCommittedCid();

    // if max_cid == MAX_CID, then it means there's no running transaction.
    if (max_cid != MAX_CID) {

      // every time we garbage collect at most 1000 tuples.
      for (size_t i = 0; i < MAX_TUPLES_PER_GC; ++i) {

        TupleMetadata tuple_metadata;
        // if there's no more tuples in the queue, then break.
        if (possibly_free_list_.Pop(tuple_metadata) == false) {
          break;
        }

        if (tuple_metadata.tuple_end_cid < max_cid) {
          // Now that we know we need to recycle tuple, we need to delete all
          // tuples from the indexes to which it belongs as well.

          DeleteTupleFromIndexes(tuple_metadata);

          auto tile_group_header =
              manager.GetTileGroup(tuple_metadata.tile_group_id)->GetHeader();

          tile_group_header->SetTransactionId(tuple_metadata.tuple_slot_id,
                                              INVALID_TXN_ID);
          tile_group_header->SetBeginCommitId(tuple_metadata.tuple_slot_id,
                                              MAX_CID);
          tile_group_header->SetEndCommitId(tuple_metadata.tuple_slot_id,
                                            MAX_CID);
          tile_group_header->SetNextItemPointer(tuple_metadata.tuple_slot_id,
                                                INVALID_ITEMPOINTER);
          tile_group_header->SetPrevItemPointer(tuple_metadata.tuple_slot_id,
                                                INVALID_ITEMPOINTER);

          std::shared_ptr<LockfreeQueue<TupleMetadata>> free_list;

          // if the entry for table_id exists.
          if (free_map_.find(tuple_metadata.table_id, free_list) == true) {
            // if the entry for tuple_metadata.table_id exists.
            free_list->Push(tuple_metadata);
          } else {
            // if the entry for tuple_metadata.table_id does not exist.
            free_list.reset(
                new LockfreeQueue<TupleMetadata>(MAX_TUPLES_PER_GC));
            free_list->Push(tuple_metadata);
            free_map_[tuple_metadata.table_id] = free_list;
          }

        } else {
          // if a tuple cannot be reclaimed, then add it back to the list.
          possibly_free_list_.Push(tuple_metadata);
        }
      }  // end for
    }    // end if
    if (is_running_ == false) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
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

  possibly_free_list_.Push(tuple_metadata);
}

// this function returns a free tuple slot, if one exists
// called by data_table.
ItemPointer GCManager::ReturnFreeSlot(const oid_t &table_id) {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return ItemPointer();
  }

  std::shared_ptr<LockfreeQueue<TupleMetadata>> free_list;
  // if there exists free_list
  if (free_map_.find(table_id, free_list) == true) {
    TupleMetadata tuple_metadata;
    if (free_list->Pop(tuple_metadata) == true) {
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

  storage::DataTable *table =
      dynamic_cast<storage::DataTable *>(tile_group->GetAbstractTable());
  assert(table != nullptr);

  std::unique_ptr<storage::Tuple> expired_tuple(
      new storage::Tuple(table->GetSchema(), true));
  tile_group->CopyTuple(tuple_metadata.tuple_slot_id, expired_tuple.get());

  for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
    auto index = table->GetIndex(idx);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(
        new storage::Tuple(table->GetSchema(), true));
    key->SetFromTuple(expired_tuple.get(), indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY: {
        auto tile_group_header = tile_group->GetHeader();
        ItemPointer next_version =
            tile_group_header->GetNextItemPointer(tuple_metadata.tuple_slot_id);
        // do we need to reset the prev_item_pointer for next_version??
        assert(next_version.IsNull() == false);

        std::vector<ItemPointerContainer *> item_pointer_containers;
        index->ScanKey(key.get(), item_pointer_containers);
        assert(item_pointer_containers.size() == 1);

        item_pointer_containers[0]->SetItemPointer(next_version);

      } break;
      default: {
        index->DeleteEntry(key.get(),
                           ItemPointer(tuple_metadata.tile_group_id,
                                       tuple_metadata.tuple_slot_id));
      }
    }

  }
}

}  // namespace gc
}  // namespace peloton
