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
/**
 * @brief Return the singleton gc manager instance
 */
GCManager &GCManager::GetInstance() {
  static GCManager gc_manager;
  return gc_manager;
}

void GCManager::StartGC(const oid_t &database_id) {
  GCContext *context = new GCContext();
  std::thread *gc_thread = new std::thread(&GCManager::Poll, this, context);
  gc_contexts_[database_id] = std::make_pair(gc_thread, context);
}

void GCManager::StopGC(const oid_t &database_id) {
  auto entry = gc_contexts_.find(database_id);
  std::thread *gc_thread = entry.first;
  GCContext *context = entry.second;
  context->is_running_ = false;
  gc_thread->join();
  gc_contexts_.erase(database_id);
  delete context;
  delete gc_thread;
}

void GCManager::Poll(GCContext *context) {
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
        if (context->possibly_free_list_.Pop(tuple_metadata) == false) {
          break;
        }

        if (tuple_metadata.tuple_end_cid < max_cid) {
          // Now that we know we need to recycle tuple, we need to delete all
          // tuples from the indexes to which it belongs as well.
          // TODO: currently, we do not delete tuple from indexes,
          // as we do not have a concurrent index yet. --Yingjun
          //DeleteTupleFromIndexes(tuple_metadata);

          auto tile_group_header = manager.GetTileGroup(tuple_metadata.tile_group_id)->GetHeader();
                    
          tile_group_header->SetTransactionId(tuple_metadata.tuple_slot_id, INVALID_TXN_ID);
          tile_group_header->SetBeginCommitId(tuple_metadata.tuple_slot_id, MAX_CID);
          tile_group_header->SetEndCommitId(tuple_metadata.tuple_slot_id, MAX_CID);
          
          LockfreeQueue<TupleMetadata> *free_list = nullptr;

          // if the entry for table_id exists.
          if (context->free_map_.find(tuple_metadata.table_id, free_list) == true) {
            // if the entry for tuple_metadata.table_id exists.
            free_list->Push(tuple_metadata);
          } else {
            // if the entry for tuple_metadata.table_id does not exist.
            free_list = new LockfreeQueue<TupleMetadata>(MAX_TUPLES_PER_GC);
            free_list->Push(tuple_metadata);
            context->free_map_[tuple_metadata.table_id] = free_list;
          }
          
        } else {
          // if a tuple can't be reaped, add it back to the list.
          context->possibly_free_list_.Push(tuple_metadata);
        }
      } // end for
    } // end if
    if (context->is_running_ == false) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
}

// this function adds a tuple to the possibly free list
void GCManager::AddPossiblyFreeTuple(const oid_t &database_id, const TupleMetadata &tuple_metadata) {
  assert(gc_contexts_.contains(database_id));

  GCContext *context = gc_contexts_.find(database_id).second;
  
  context->possibly_free_list_.Push(tuple_metadata);
}

// this function returns a free tuple slot, if one exists
ItemPointer GCManager::ReturnFreeSlot(const oid_t &database_id, const oid_t &table_id) {
  assert(gc_contexts_.contains(database_id));

  GCContext *context = gc_contexts_.find(database_id).second;

  ItemPointer ret_item_pointer;
  
  LockfreeQueue<TupleMetadata> *free_list = nullptr;
  // if there exists free_list
  if (context->free_map_.find(table_id, free_list) == true) {
    TupleMetadata tuple_metadata;
    if (free_list->Pop(tuple_metadata) == true) {
      ret_item_pointer.block = tuple_metadata.tile_group_id;
      ret_item_pointer.offset = tuple_metadata.tuple_slot_id;
    }
  }
  return ret_item_pointer;
}

// delete a tuple from all its indexes it belongs in
void GCManager::DeleteTupleFromIndexes(const TupleMetadata &tuple_metadata) {
  auto &manager = catalog::Manager::GetInstance();
  auto db = manager.GetDatabaseWithOid(tuple_metadata.database_id);
  auto table = db->GetTableWithOid(tuple_metadata.table_id);
  auto index_count = table->GetIndexCount();
  auto tile_group = manager.GetTileGroup(tuple_metadata.tile_group_id).get();
  auto tile_count = tile_group->GetTileCount();
  for (oid_t i = 0; i < tile_count; i++) {
    auto tile = tile_group->GetTile(i);
    for (oid_t j = 0; j < index_count; j++) {
      // delete tuple from each index
      auto index = table->GetIndex(j);
      ItemPointer item(tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id);
      auto index_schema = index->GetKeySchema();
      auto indexed_columns = index_schema->GetIndexedColumns();
      std::unique_ptr<storage::Tuple> key(
          new storage::Tuple(index_schema, true));
      char *tile_tuple_location = tile->GetTupleLocation(tuple_metadata.tuple_slot_id);
      assert(tile_tuple_location);
      storage::Tuple tuple(tile->GetSchema(), tile_tuple_location);
      key->SetFromTuple(&tuple, indexed_columns, index->GetPool());
      index->DeleteEntry(key.get(), item);
    }
  }
}

}  // namespace gc
}  // namespace peloton
