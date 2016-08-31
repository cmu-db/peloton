//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_level_gc_manager.cpp
//
// Identification: src/gc/transaction_level_gc_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gc/transaction_level_gc_manager.h"
#include "storage/tuple.h"
#include "storage/database.h"
#include "storage/tile_group.h"
#include "catalog/manager.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/container_tuple.h"

namespace peloton {
namespace gc {

void TransactionLevelGCManager::StartGC(int thread_id) {
  LOG_TRACE("Starting GC");
  this->is_running_ = true;
  gc_threads_[thread_id].reset(new std::thread(&TransactionLevelGCManager::Running, this, thread_id));
}

void TransactionLevelGCManager::StopGC(int thread_id) {
  LOG_TRACE("Stopping GC");
  this->is_running_ = false;
  this->gc_threads_[thread_id]->join();
  ClearGarbage(thread_id);
}

bool TransactionLevelGCManager::ResetTuple(const ItemPointer &location) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);

  // During the resetting, a table may deconstruct because of the DROP TABLE request
  if (tile_group == nullptr) {
    return false;
  }

  auto tile_group_header = tile_group->GetHeader();

  // Reset the header
  tile_group_header->SetTransactionId(location.offset, INVALID_TXN_ID);
  tile_group_header->SetBeginCommitId(location.offset, MAX_CID);
  tile_group_header->SetEndCommitId(location.offset, MAX_CID);
  tile_group_header->SetPrevItemPointer(location.offset, INVALID_ITEMPOINTER);
  tile_group_header->SetNextItemPointer(location.offset, INVALID_ITEMPOINTER);
  PL_MEMSET(
    tile_group_header->GetReservedFieldRef(location.offset), 0,
    storage::TileGroupHeader::GetReservedSize());

  LOG_TRACE("Garbage tuple(%u, %u) is reset", location.block, location.offset);
  return true;
}

void TransactionLevelGCManager::Running(const int &thread_id) {

  while (true) {

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto max_cid = txn_manager.GetMaxCommittedCid();

    PL_ASSERT(max_cid != MAX_CID);

    Reclaim(thread_id, max_cid);

    Unlink(thread_id, max_cid);

    if (is_running_ == false) {
      return;
    }
  }
}


void TransactionLevelGCManager::RegisterTransaction(const RWSet &rw_set, const cid_t &timestamp) {
    // Add the garbage context to the lockfree queue
    std::shared_ptr<GarbageContext> gc_context(new GarbageContext(rw_set, timestamp));
    unlink_queues_[HashToThread(gc_context->timestamp_)]->Enqueue(gc_context);
}

void TransactionLevelGCManager::Unlink(const int &thread_id, const cid_t &max_cid) {
  
  int tuple_counter = 0;

  // check if any garbage can be unlinked from indexes.
  // every time we garbage collect at most MAX_ATTEMPT_COUNT tuples.
  std::vector<std::shared_ptr<GarbageContext>> garbages;

  // First iterate the local unlink queue
  local_unlink_queues_[thread_id].remove_if(
    [this, &garbages, &tuple_counter, max_cid](const std::shared_ptr<GarbageContext>& garbage_ctx) -> bool {
      bool res = garbage_ctx->timestamp_ < max_cid;
      if (res) {
        for (auto entry : garbage_ctx->rw_set_) {
          for (auto &element : entry.second) {
            if (element.second == RW_TYPE_UPDATE) {
              DeleteTupleFromIndexes(ItemPointer(entry.first, element.first)); 
            }
          }
        }
        garbages.push_back(garbage_ctx);
        tuple_counter++;
      }
      return res;
    }
  );

  for (size_t i = 0; i < MAX_ATTEMPT_COUNT; ++i) {

    std::shared_ptr<GarbageContext> garbage_ctx;
    // if there's no more tuples in the queue, then break.
    if (unlink_queues_[thread_id]->Dequeue(garbage_ctx) == false) {
      break;
    }

    if (garbage_ctx->timestamp_ < max_cid) {
      // Now that we know we need to recycle tuple, we need to delete all
      // tuples from the indexes to which it belongs as well.
      for (auto &entry : garbage_ctx->rw_set_) {
        for (auto &element : entry.second) {
          if (element.second == RW_TYPE_UPDATE) {
            DeleteTupleFromIndexes(ItemPointer(entry.first, element.first)); 
          }
        }
      }
      // Add to the garbage map
      garbages.push_back(garbage_ctx);
      tuple_counter++;

    } else {
      // if a tuple cannot be reclaimed, then add it back to the list.
      local_unlink_queues_[thread_id].push_back(garbage_ctx);
    }
  }  // end for

  auto safe_max_cid = concurrency::TransactionManagerFactory::GetInstance().GetNextCommitId();
  for(auto& item : garbages){
      reclaim_maps_[thread_id].insert(std::make_pair(safe_max_cid, item));
  }
  LOG_TRACE("Marked %d tuples as garbage", tuple_counter);
}

// executed by a single thread. so no synchronization is required.
void TransactionLevelGCManager::Reclaim(const int &thread_id, const cid_t &max_cid) {
  int gc_counter = 0;

  // we delete garbage in the free list
  auto garbage_ctx = reclaim_maps_[thread_id].begin();
  while (garbage_ctx != reclaim_maps_[thread_id].end()) {
    const cid_t garbage_ts = garbage_ctx->first;
    auto garbage_context = garbage_ctx->second;

    // if the timestamp of the garbage is older than the current max_cid,
    // recycle it
    if (garbage_ts < max_cid) {
      AddToRecycleMap(garbage_context);

      // Remove from the original map
      garbage_ctx = reclaim_maps_[thread_id].erase(garbage_ctx);
      gc_counter++;
    } else {
      // Early break since we use an ordered map
      break;
    }
  }
  LOG_TRACE("Marked %d txn contexts as recycled", gc_counter);
}

// Multiple GC thread share the same recycle map
void TransactionLevelGCManager::AddToRecycleMap(std::shared_ptr<GarbageContext> gc_ctx) {
  // If the tuple being reset no longer exists, just skip it
  for (auto &entry : gc_ctx->rw_set_) {
    for (auto &element : entry.second) {

      auto &manager = catalog::Manager::GetInstance();
      auto tile_group = manager.GetTileGroup(entry.first);

      PL_ASSERT(tile_group != nullptr);

      storage::DataTable *table =
        dynamic_cast<storage::DataTable *>(tile_group->GetAbstractTable());
      PL_ASSERT(table != nullptr);

      oid_t table_id = table->GetOid();

      if (element.second == RW_TYPE_UPDATE) {
        ItemPointer location(entry.first, element.first); 

        if (ResetTuple(location) == false) {
          continue;
        }
        // if the entry for table_id exists.
        PL_ASSERT(recycle_queue_map_.find(table_id) != recycle_queue_map_.end());
        recycle_queue_map_[table_id]->Enqueue(location);
      }
    }
  }
}

// this function returns a free tuple slot, if one exists
// called by data_table.
ItemPointer TransactionLevelGCManager::ReturnFreeSlot(const oid_t &table_id) {
  PL_ASSERT(recycle_queue_map_.count(table_id) != 0);
  ItemPointer location;
  auto recycle_queue = recycle_queue_map_[table_id];

  if (recycle_queue->Dequeue(location) == true) {
    LOG_TRACE("Reuse tuple(%u, %u) in table %u", location.block,
              location.offset, table_id);
    return location;
  }
  return INVALID_ITEMPOINTER;
}


void TransactionLevelGCManager::ClearGarbage(int thread_id) {
  while(!unlink_queues_[thread_id]->IsEmpty() || !local_unlink_queues_[thread_id].empty()) {
    Unlink(thread_id, MAX_CID);
  }

  while(reclaim_maps_[thread_id].size() != 0) {
    Reclaim(thread_id, MAX_CID);
  }

  return;
}

// delete a tuple from all its indexes it belongs to.
void TransactionLevelGCManager::DeleteTupleFromIndexes(const ItemPointer &location) {
  LOG_TRACE("Deleting index for tuple(%u, %u)", location.block, location.offset);
  
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);

  PL_ASSERT(tile_group != nullptr);

  storage::DataTable *table =
    dynamic_cast<storage::DataTable *>(tile_group->GetAbstractTable());
  PL_ASSERT(table != nullptr);
  
  if (table->GetIndexCount() == 1) {
    // in this case, the table only has primary index. do nothing.
    return;
  }

  // construct the expired version.
  expression::ContainerTuple<storage::TileGroup> expired_tuple(tile_group.get(), location.offset);

  // unlink the version from all the indexes.
  for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
    auto index = table->GetIndex(idx);
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    // build key.
    std::unique_ptr<storage::Tuple> key(
      new storage::Tuple(index_schema, true));
    key->SetFromTuple(&expired_tuple, indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
      case INDEX_CONSTRAINT_TYPE_PRIMARY_KEY:
        break;
      default: {
        LOG_TRACE("Deleting other index");
        index->DeleteEntry(key.get(), new ItemPointer(location));
      }
    }
  }
}


}  // namespace gc
}  // namespace peloton
