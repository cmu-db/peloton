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
#include "common/container_tuple.h"

namespace peloton {
namespace gc {

bool TransactionLevelGCManager::ResetTuple(const ItemPointer &location) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block).get();

  auto tile_group_header = tile_group->GetHeader();

  // Reset the header
  tile_group_header->SetTransactionId(location.offset, INVALID_TXN_ID);
  tile_group_header->SetBeginCommitId(location.offset, MAX_CID);
  tile_group_header->SetEndCommitId(location.offset, MAX_CID);
  tile_group_header->SetPrevItemPointer(location.offset, INVALID_ITEMPOINTER);
  tile_group_header->SetNextItemPointer(location.offset, INVALID_ITEMPOINTER);

  PL_MEMSET(tile_group_header->GetReservedFieldRef(location.offset), 0,
            storage::TileGroupHeader::GetReservedSize());

  // Reclaim the varlen pool
  CheckAndReclaimVarlenColumns(tile_group, location.offset);

  LOG_TRACE("Garbage tuple(%u, %u) is reset", location.block, location.offset);
  return true;
}

void TransactionLevelGCManager::Running(const int &thread_id) {
  PL_ASSERT(is_running_ == true);
  uint32_t backoff_shifts = 0;
  while (true) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto max_cid = txn_manager.GetMaxCommittedCid();

    // When the DBMS has started working but it never processes any transaction,
    // we may see max_cid == MAX_CID.
    if (max_cid == MAX_CID) {
      continue;
    }

    int reclaimed_count = Reclaim(thread_id, max_cid);

    int unlinked_count = Unlink(thread_id, max_cid);

    if (is_running_ == false) {
      return;
    }
    if (reclaimed_count == 0 && unlinked_count == 0) {
      // sleep at most 0.8192 s
      if (backoff_shifts < 13) {
        ++backoff_shifts;
      }
      uint64_t sleep_duration = 1UL << backoff_shifts;
      sleep_duration *= 100;
      std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
    } else {
      backoff_shifts >>= 1;
    }
  }
}

void TransactionLevelGCManager::RecycleTransaction(
    std::shared_ptr<GCSet> gc_set, const cid_t &timestamp) {
  // Add the garbage context to the lock-free queue
  std::shared_ptr<GarbageContext> gc_context(
      new GarbageContext(gc_set, timestamp));
  unlink_queues_[HashToThread(gc_context->timestamp_)]->Enqueue(gc_context);
}

int TransactionLevelGCManager::Unlink(const int &thread_id,
                                      const cid_t &max_cid) {
  int tuple_counter = 0;

  // check if any garbage can be unlinked from indexes.
  // every time we garbage collect at most MAX_ATTEMPT_COUNT tuples.
  std::vector<std::shared_ptr<GarbageContext>> garbages;

  // First iterate the local unlink queue
  local_unlink_queues_[thread_id].remove_if(
      [this, &garbages, &tuple_counter, max_cid](
          const std::shared_ptr<GarbageContext> &garbage_ctx) -> bool {
        bool res = garbage_ctx->timestamp_ < max_cid;
        if (res == true) {
          DeleteFromIndexes(garbage_ctx);
          // Add to the garbage map

          garbages.push_back(garbage_ctx);
          tuple_counter++;
        }
        return res;
      });

  for (size_t i = 0; i < MAX_ATTEMPT_COUNT; ++i) {
    std::shared_ptr<GarbageContext> garbage_ctx;
    // if there's no more tuples in the queue, then break.
    if (unlink_queues_[thread_id]->Dequeue(garbage_ctx) == false) {
      break;
    }

    if (garbage_ctx->timestamp_ < max_cid) {
      // as the max timestamp of committed transactions is larger than the gc's
      // timestamp,
      // it means that no active transactions can read it.
      // so we can unlink it.
      // we need to delete all the tuples from the indexes to which it belongs
      // as well.
      DeleteFromIndexes(garbage_ctx);
      // Add to the garbage map
      garbages.push_back(garbage_ctx);
      tuple_counter++;

    } else {
      // if a tuple cannot be reclaimed, then add it back to the list.
      local_unlink_queues_[thread_id].push_back(garbage_ctx);
    }
  }  // end for

  auto safe_max_cid =
      concurrency::TransactionManagerFactory::GetInstance().GetNextCommitId();
  for (auto &item : garbages) {
    reclaim_maps_[thread_id].insert(std::make_pair(safe_max_cid, item));
  }
  return tuple_counter;
}

// executed by a single thread. so no synchronization is required.
int TransactionLevelGCManager::Reclaim(const int &thread_id,
                                       const cid_t &max_cid) {
  int gc_counter = 0;

  // we delete garbage in the free list
  auto garbage_ctx_entry = reclaim_maps_[thread_id].begin();
  while (garbage_ctx_entry != reclaim_maps_[thread_id].end()) {
    const cid_t garbage_ts = garbage_ctx_entry->first;
    auto garbage_ctx = garbage_ctx_entry->second;

    // if the timestamp of the garbage is older than the current max_cid,
    // recycle it
    if (garbage_ts < max_cid) {
      AddToRecycleMap(garbage_ctx);

      // Remove from the original map
      garbage_ctx_entry = reclaim_maps_[thread_id].erase(garbage_ctx_entry);
      gc_counter++;
    } else {
      // Early break since we use an ordered map
      break;
    }
  }
  return gc_counter;
}

// Multiple GC thread share the same recycle map
void TransactionLevelGCManager::AddToRecycleMap(
    std::shared_ptr<GarbageContext> garbage_ctx) {
  for (auto &entry : *(garbage_ctx->gc_set_.get())) {
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(entry.first);

    // During the resetting, a table may be deconstructed because of the DROP
    // TABLE request
    if (tile_group == nullptr) {
      return;
    }

    PL_ASSERT(tile_group != nullptr);

    storage::DataTable *table =
        dynamic_cast<storage::DataTable *>(tile_group->GetAbstractTable());
    PL_ASSERT(table != nullptr);

    oid_t table_id = table->GetOid();

    for (auto &element : entry.second) {
      // as this transaction has been committed, we should reclaim older
      // versions.
      ItemPointer location(entry.first, element.first);

      // If the tuple being reset no longer exists, just skip it
      if (ResetTuple(location) == false) {
        continue;
      }
      // if the entry for table_id exists.
      if (recycle_queue_map_.find(table_id) != recycle_queue_map_.end()) {
        recycle_queue_map_[table_id]->Enqueue(location);
      }
    }
  }
}

// this function returns a free tuple slot, if one exists
// called by data_table.
ItemPointer TransactionLevelGCManager::ReturnFreeSlot(const oid_t &table_id) {
  // for catalog tables, we directly return invalid item pointer.
  if (recycle_queue_map_.find(table_id) == recycle_queue_map_.end()) {
    return INVALID_ITEMPOINTER;
  }
  ItemPointer location;
  PL_ASSERT(recycle_queue_map_.find(table_id) != recycle_queue_map_.end());
  auto recycle_queue = recycle_queue_map_[table_id];

  if (recycle_queue->Dequeue(location) == true) {
    LOG_TRACE("Reuse tuple(%u, %u) in table %u", location.block,
              location.offset, table_id);
    return location;
  }
  return INVALID_ITEMPOINTER;
}

void TransactionLevelGCManager::ClearGarbage(int thread_id) {
  while (!unlink_queues_[thread_id]->IsEmpty() ||
         !local_unlink_queues_[thread_id].empty()) {
    Unlink(thread_id, MAX_CID);
  }

  while (reclaim_maps_[thread_id].size() != 0) {
    Reclaim(thread_id, MAX_CID);
  }

  return;
}

void TransactionLevelGCManager::DeleteFromIndexes(
    const std::shared_ptr<GarbageContext> &garbage_ctx) {
  for (auto entry : *(garbage_ctx->gc_set_.get())) {
    for (auto &element : entry.second) {
      if (element.second == true) {
        // only old versions are stored in the gc set.
        // so we can safely get indirection from the indirection array.
        auto tile_group =
            catalog::Manager::GetInstance().GetTileGroup(entry.first);
        if (tile_group != nullptr) {
          auto tile_group_header = catalog::Manager::GetInstance()
                                       .GetTileGroup(entry.first)
                                       ->GetHeader();
          ItemPointer *indirection =
              tile_group_header->GetIndirection(element.first);

          DeleteTupleFromIndexes(indirection);
        }
      }
    }
  }
}

// delete a tuple from all its indexes it belongs to.
void TransactionLevelGCManager::DeleteTupleFromIndexes(
    ItemPointer *indirection) {
  // do nothing if indirection is null
  if (indirection == nullptr) {
    return;
  }
  LOG_TRACE("Deleting indirection %p from index", indirection);

  ItemPointer location = *indirection;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);

  PL_ASSERT(tile_group != nullptr);

  storage::DataTable *table =
      dynamic_cast<storage::DataTable *>(tile_group->GetAbstractTable());
  PL_ASSERT(table != nullptr);

  // construct the expired version.
  expression::ContainerTuple<storage::TileGroup> expired_tuple(tile_group.get(),
                                                               location.offset);

  // unlink the version from all the indexes.
  for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
    auto index = table->GetIndex(idx);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    // build key.
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(&expired_tuple, indexed_columns, index->GetPool());

    index->DeleteEntry(key.get(), indirection);
  }
}

}  // namespace gc
}  // namespace peloton
