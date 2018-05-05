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

#include "brain/query_logger.h"
#include "catalog/manager.h"
#include "catalog/catalog.h"
#include "common/container_tuple.h"
#include "concurrency/epoch_manager_factory.h"
#include "concurrency/transaction_manager_factory.h"
#include "index/index.h"
#include "settings/settings_manager.h"
#include "storage/database.h"
#include "storage/storage_manager.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"
#include "threadpool/mono_queue_pool.h"


namespace peloton {
namespace gc {

// Assumes that location is valid
bool TransactionLevelGCManager::ResetTuple(const ItemPointer &location) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block).get();

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

  PELOTON_MEMSET(tile_group_header->GetReservedFieldRef(location.offset), 0,
            storage::TileGroupHeader::GetReservedSize());

  // Reclaim the varlen pool
  CheckAndReclaimVarlenColumns(tile_group, location.offset);

  LOG_TRACE("Garbage tuple(%u, %u) is reset", location.block, location.offset);
  return true;
}

void TransactionLevelGCManager::Running(const int &thread_id) {
  PELOTON_ASSERT(is_running_ == true);
  uint32_t backoff_shifts = 0;
  while (true) {
    auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

    auto expired_eid = epoch_manager.GetExpiredEpochId();

    // When the DBMS has started working but it never processes any transaction,
    // we may see expired_eid == MAX_EID.
    if (expired_eid == MAX_EID) {
      continue;
    }

    int reclaimed_count = Reclaim(thread_id, expired_eid);
    int unlinked_count = Unlink(thread_id, expired_eid);

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
    concurrency::TransactionContext *txn) {
  auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

  epoch_manager.ExitEpoch(txn->GetThreadId(),
                          txn->GetEpochId());

  if (txn->GetIsolationLevel() != IsolationLevelType::READ_ONLY && \
      txn->GetResult() != ResultType::SUCCESS && txn->IsGCSetEmpty() != true) {
        txn->SetEpochId(epoch_manager.GetNextEpochId());
  }

  // Add the transaction context to the lock-free queue
  unlink_queues_[HashToThread(txn->GetThreadId())]->Enqueue(txn);
}

int TransactionLevelGCManager::Unlink(const int &thread_id,
                                      const eid_t &expired_eid) {
  int tuple_counter = 0;

  // check if any garbage can be unlinked from indexes.
  // every time we garbage collect at most MAX_ATTEMPT_COUNT tuples.
  std::vector<concurrency::TransactionContext* > garbages;

  // First iterate the local unlink queue
  local_unlink_queues_[thread_id].remove_if(
      [&garbages, &tuple_counter, expired_eid,
       this](concurrency::TransactionContext *txn_ctx) -> bool {
        bool res = txn_ctx->GetEpochId() <= expired_eid;
        if (res == true) {
          // unlink versions from version chain and indexes
          UnlinkVersions(txn_ctx);
          // Add to the garbage map
          garbages.push_back(txn_ctx);
          tuple_counter++;
        }
        return res;
      });

  for (size_t i = 0; i < MAX_ATTEMPT_COUNT; ++i) {
    concurrency::TransactionContext *txn_ctx;
    // if there's no more tuples in the queue, then break.
    if (unlink_queues_[thread_id]->Dequeue(txn_ctx) == false) {
      break;
    }

    // Log the query into query_history_catalog
    if (settings::SettingsManager::GetBool(settings::SettingId::brain)) {
      std::vector<std::string> query_strings = txn_ctx->GetQueryStrings();
      if(query_strings.size() != 0) {
        uint64_t timestamp = txn_ctx->GetTimestamp();
        auto &pool = threadpool::MonoQueuePool::GetBrainInstance();
        for(auto query_string: query_strings) {
          pool.SubmitTask([query_string, timestamp] {
            brain::QueryLogger::LogQuery(query_string, timestamp);
          });        
        }
      }
    }

    // Deallocate the Transaction Context of transactions that don't involve
    // any garbage collection
    if (txn_ctx->GetIsolationLevel() == IsolationLevelType::READ_ONLY || \
        txn_ctx->IsGCSetEmpty()) {
      delete txn_ctx;
      continue;
    }

    if (txn_ctx->GetEpochId() <= expired_eid) {
      // as the global expired epoch id is no less than the garbage version's
      // epoch id, it means that no active transactions can read the version. As
      // a result, we can delete all the tuples from the indexes to which it
      // belongs.

      // unlink versions from version chain and indexes
      UnlinkVersions(txn_ctx);
      // Add to the garbage map
      garbages.push_back(txn_ctx);
      tuple_counter++;

    } else {
      // if a tuple cannot be reclaimed, then add it back to the list.
      local_unlink_queues_[thread_id].push_back(txn_ctx);
    }
  }  // end for

  // once the current epoch id is expired, then we know all the transactions
  // that are active at this time point will be committed/aborted.
  // at that time point, it is safe to recycle the version.
  eid_t safe_expired_eid =
      concurrency::EpochManagerFactory::GetInstance().GetCurrentEpochId();

  for (auto &item : garbages) {
    reclaim_maps_[thread_id].insert(std::make_pair(safe_expired_eid, item));
  }
  LOG_TRACE("Marked %d tuples as garbage", tuple_counter);
  return tuple_counter;
}

// executed by a single thread. so no synchronization is required.
int TransactionLevelGCManager::Reclaim(const int &thread_id,
                                       const eid_t &expired_eid) {
  int gc_counter = 0;

  // we delete garbage in the free list
  auto garbage_ctx_entry = reclaim_maps_[thread_id].begin();
  while (garbage_ctx_entry != reclaim_maps_[thread_id].end()) {
    const eid_t garbage_eid = garbage_ctx_entry->first;
    auto txn_ctx = garbage_ctx_entry->second;

    // if the global expired epoch id is no less than the garbage version's
    // epoch id, then recycle the garbage version
    if (garbage_eid <= expired_eid) {
      AddToRecycleMap(txn_ctx);

      // Remove from the original map
      garbage_ctx_entry = reclaim_maps_[thread_id].erase(garbage_ctx_entry);
      gc_counter++;
    } else {
      // Early break since we use an ordered map
      break;
    }
  }
  LOG_TRACE("Marked %d txn contexts as recycled", gc_counter);
  return gc_counter;
}

// Multiple GC threads share the same recycle map
void TransactionLevelGCManager::AddToRecycleMap(
    concurrency::TransactionContext* txn_ctx) {

  // for each tile group that this txn created garbage tuples in
  for (auto &entry : *(txn_ctx->GetGCSetPtr().get())) {
    auto tile_group_id = entry.first;
    auto tile_group = catalog::Manager::GetInstance().GetTileGroup(tile_group_id);

    // During the resetting,
    // a table may be deconstructed because of a DROP TABLE request
    if (tile_group == nullptr) {
      // try to process any remaining tile groups from this txn
      continue;
    }

    auto table = storage::StorageManager::GetInstance()->GetTableWithOid(tile_group->GetDatabaseId(), tile_group->GetTableId());

    if (table == nullptr) {
      // Guard against the table being dropped out from under us
      continue;
    }

    oid_t table_id = table->GetOid();
    auto tile_group_header = tile_group->GetHeader();
    tile_group_header->IncrementGCReaders();

    // for each garbage tuple in the Tile Group
    for (auto &element : entry.second) {
      auto offset = element.first;
      ItemPointer location(tile_group_id, offset);

      // TODO: Ensure that immutable checks are compatible with GetRecycledTupleSlot's behavior
      // Currently, we rely on GetRecycledTupleSlot to ignore immutable slots
      // TODO: revisit queueing immutable ItemPointers
      // TODO: revisit dropping immutable tile groups

      // If the tuple being reset no longer exists, just skip it
      if (ResetTuple(location) == false) {
        continue;
      }

      auto recycle_queue = GetTableRecycleQueue(table_id);
      if (recycle_queue == nullptr) {
        continue;
      }
      auto num_recycled = tile_group_header->IncrementRecycled() + 1;
      auto tuples_per_tile_group = table->GetTuplesPerTileGroup();

      // tunable knob, 50% for now
      auto recycling_threshold = tuples_per_tile_group >> 1;
      // tunable knob, set at 87.5% for now
      auto compaction_threshold = tuples_per_tile_group - (tuples_per_tile_group >> 3);

      bool recycling = tile_group_header->GetRecycling();

      // check if recycling should be disabled (and if tile group should be compacted)
      if (num_recycled >= recycling_threshold &&
          table->IsActiveTileGroup(tile_group_id) == false) {

        if (recycling) {
          tile_group_header->StopRecycling();
          recycling = false;
        }

        if (num_recycled >= compaction_threshold) {
          // TODO: compact this tile group
        }
      }

      if (recycling) {
        // this slot should be recycled, add it back to the recycle queue
        recycle_queue->Enqueue(location);
      }

      // Check if tile group should be freed
      if (num_recycled == tuples_per_tile_group && recycling == false) {
        // This GC thread should free the TileGroup
        while (tile_group_header->GetGCReaders() > 1) {
          // Spin here until the other GC threads stop operating on this TileGroup
        }
        table->DropTileGroup(tile_group_id);

        // TODO: clean the recycle queue of this TileGroup's ItemPointers
        // RemoveInvalidSlotsFromRecycleQueue(recycle_queue, tile_group_id);
        // For now, we'll rely on GetRecycledTupleSlot to consume and ignore invalid slots
      }
    }
    tile_group_header->DecrementGCReaders();
  }

  // Perform object-level GC (e.g. dropped tables, indexes, databases)
  auto storage_manager = storage::StorageManager::GetInstance();
  for (auto &entry : *(txn_ctx->GetGCObjectSetPtr().get())) {
    oid_t database_oid = std::get<0>(entry);
    oid_t table_oid = std::get<1>(entry);
    oid_t index_oid = std::get<2>(entry);
    PELOTON_ASSERT(database_oid != INVALID_OID);
    auto database = storage_manager->GetDatabaseWithOid(database_oid);
    PELOTON_ASSERT(database != nullptr);
    if (table_oid == INVALID_OID) {
      storage_manager->RemoveDatabaseFromStorageManager(database_oid);
      continue;
    }
    auto table = database->GetTableWithOid(table_oid);
    PELOTON_ASSERT(table != nullptr);
    if (index_oid == INVALID_OID) {
      database->DropTableWithOid(table_oid);
      LOG_DEBUG("GCing table %u", table_oid);
      continue;
    }
    auto index = table->GetIndexWithOid(index_oid);
    PELOTON_ASSERT(index != nullptr);
    table->DropIndexWithOid(index_oid);
    LOG_DEBUG("GCing index %u", index_oid);
  }

  delete txn_ctx;
}

// This function currently replicates a lot functionality in AddToRecyleMap
// These will likely be merged in later PR
void TransactionLevelGCManager::RecycleUnusedTupleSlot(storage::DataTable *table, const ItemPointer &location) {
  if (table == nullptr) {
    return;
  }

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);

  // a table may be deconstructed because of a DROP TABLE request
  if (tile_group == nullptr) {
    // try to process any remaining tile groups from this txn
    return;
  }

  oid_t table_id = table->GetOid();
  auto tile_group_header = tile_group->GetHeader();

  tile_group_header->IncrementGCReaders();

  // TODO: Ensure that immutable checks are compatible with GetRecycledTupleSlot's behavior
  // Currently, we rely on GetRecycledTupleSlot to ignore immutable slots
  // TODO: revisit queueing immutable ItemPointers
  // TODO: revisit dropping immutable tile groups

  // If the tuple being reset no longer exists, just skip it
  if (ResetTuple(location) == false) {
    return;
  }

  auto recycle_queue = GetTableRecycleQueue(table_id);
  if (recycle_queue == nullptr) {
    return;
  }
  auto num_recycled = tile_group_header->IncrementRecycled() + 1;
  auto tuples_per_tile_group = table->GetTuplesPerTileGroup();

  // tunable knob, 50% for now
  auto recycling_threshold = tuples_per_tile_group >> 1;
  // tunable knob, set at 87.5% for now
  auto compaction_threshold = tuples_per_tile_group - (tuples_per_tile_group >> 3);

  bool recycling = tile_group_header->GetRecycling();

  // check if recycling should be disabled (and if tile group should be compacted)
  if (num_recycled >= recycling_threshold &&
      table->IsActiveTileGroup(location.block) == false) {

    if (recycling) {
      tile_group_header->StopRecycling();
      recycling = false;
    }

    if (num_recycled >= compaction_threshold) {
      // TODO: compact this tile group
    }
  }

  if (recycling) {
    // this slot should be recycled, add it back to the recycle queue
    recycle_queue->Enqueue(location);
  }

  // Check if tile group should be freed
  if (num_recycled == tuples_per_tile_group && recycling == false) {
    // This GC thread should free the TileGroup
    while (tile_group_header->GetGCReaders() > 1) {
      // Spin here until the other GC threads stop operating on this TileGroup
    }
    table->DropTileGroup(location.block);

    // TODO: clean the recycle queue of this TileGroup's ItemPointers
    // RemoveInvalidSlotsFromRecycleQueue(recycle_queue, tile_group_id);
    // For now, we'll rely on GetRecycledTupleSlot to consume and ignore invalid slots
  }
  tile_group_header->DecrementGCReaders();
}

// returns a free tuple slot that can now be recycled/reused, if one exists
// called by data_table.
ItemPointer TransactionLevelGCManager::GetRecycledTupleSlot(storage::DataTable *table) {
  if (table == nullptr) {
    return INVALID_ITEMPOINTER;
  }
  auto table_id = table->GetOid();
  std::shared_ptr<peloton::LockFreeQueue<ItemPointer>> recycle_queue;

  if (recycle_queues_->Find(table_id, recycle_queue) == false) {
    // Table does not have a recycle queue, likely a catalog table
    return INVALID_ITEMPOINTER;
  }

  ItemPointer location;
  // Search for a slot that can be recycled
  // TODO: We're relying on GetRecycledTupleSlot to clean the recycle queue. Fix this later.
  while (recycle_queue->Dequeue(location) == true) {
    auto tile_group_id = location.block;
    auto tile_group = table->GetTileGroupById(tile_group_id);

    if (tile_group == nullptr) {
      // TileGroup no longer exists
      // return INVALID_ITEMPOINTER;
      continue;
    }

    auto tile_group_header = tile_group->GetHeader();
    bool recycling = tile_group_header->GetRecycling();
    bool immutable = tile_group_header->GetImmutability();

    if (recycling == false) {
      // Don't decrement because we want the recycled count to be our indicator to release the TileGroup
      // return INVALID_ITEMPOINTER;
      continue;
    }

    if (immutable == true) {
      // TODO: revisit queueing immutable ItemPointers, currently test expects this behavior
      // recycle_queue->Enqueue(location);
      // return INVALID_ITEMPOINTER;
      continue;

    } else {
      LOG_TRACE("Reuse tuple(%u, %u) in table %u", tile_group_id,
                location.offset, table_id);
      tile_group_header->DecrementRecycled();
      return location;
    }
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

void TransactionLevelGCManager::StopGC() {
  LOG_TRACE("Stopping GC");
  this->is_running_ = false;
  // clear the garbage in each GC thread
  for (int thread_id = 0; thread_id < gc_thread_count_; ++thread_id) {
    ClearGarbage(thread_id);
  }
}

void TransactionLevelGCManager::UnlinkVersions(
    concurrency::TransactionContext *txn_ctx) {

  // for each tile group that this txn created garbage tuples in
  for (auto entry : *(txn_ctx->GetGCSetPtr().get())) {
    auto tile_group_id = entry.first;
    auto garbage_tuples = entry.second;

    // for each garbage tuple in the tile group
    for (auto &element : garbage_tuples) {
      auto offset = element.first;
      auto gc_type = element.second;
      UnlinkVersion(ItemPointer(tile_group_id, offset), gc_type);
    }

  }
}

// unlink garbage tuples and update indexes appropriately (according to gc type)
void TransactionLevelGCManager::UnlinkVersion(const ItemPointer location,
                                              GCVersionType type) {
  // get indirection from the indirection array.
  auto tile_group =
      catalog::Manager::GetInstance().GetTileGroup(location.block);

  // if the corresponding tile group is deconstructed,
  // then do nothing.
  if (tile_group == nullptr) {
    return;
  }

  auto tile_group_header = tile_group->GetHeader();

  ItemPointer *indirection = tile_group_header->GetIndirection(location.offset);

  // do nothing if indirection is null
  if (indirection == nullptr) {
    return;
  }

  ContainerTuple<storage::TileGroup> current_tuple(tile_group.get(), location.offset);

  storage::DataTable *table =
      dynamic_cast<storage::DataTable *>(tile_group->GetAbstractTable());
  PELOTON_ASSERT(table != nullptr);

  if (type == GCVersionType::COMMIT_UPDATE) {
    // the gc'd version is an old version.
    // this old version needs to be reclaimed by the GC.
    // if this old version differs from the newest version in some columns that
    // secondary indexes are built on, then we need to delete this old version
    // from those secondary indexes

    ContainerTuple<storage::TileGroup> older_tuple(tile_group.get(),
                                                     location.offset);

    ItemPointer newer_location = tile_group_header->GetPrevItemPointer(location.offset);

    if (newer_location == INVALID_ITEMPOINTER) {
      return;
    }

    auto newer_tile_group = catalog::Manager::GetInstance().GetTileGroup(newer_location.block);
    ContainerTuple<storage::TileGroup> newer_tuple(newer_tile_group.get(), newer_location.offset);
    // remove the older version from all the indexes
    // where it no longer matches the newer version
    for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
      auto index = table->GetIndex(idx);
      if (index == nullptr) continue;
      auto index_schema = index->GetKeySchema();
      auto indexed_columns = index_schema->GetIndexedColumns();

      // build keys
      std::unique_ptr<storage::Tuple> older_key(new storage::Tuple(index_schema, true));
      older_key->SetFromTuple(&older_tuple, indexed_columns, index->GetPool());
      std::unique_ptr<storage::Tuple> newer_key(new storage::Tuple(index_schema, true));
      newer_key->SetFromTuple(&newer_tuple, indexed_columns,index->GetPool());

      // if older_key is different, delete it from index
      if (newer_key->Compare(*older_key) != 0) {
        index->DeleteEntry(older_key.get(), indirection);
      }
    }

  } else if (type == GCVersionType::ABORT_UPDATE) {
    // the gc'd version is a newly created version.
    // if the version differs from the previous one in some columns where
    // secondary indexes are built on, then we need to unlink this version
    // from the secondary index.

    ContainerTuple<storage::TileGroup> newer_tuple(tile_group.get(), location.offset);

    ItemPointer older_location =
        tile_group_header->GetNextItemPointer(location.offset);

    if (older_location == INVALID_ITEMPOINTER) {
      return;
    }

    auto older_tile_group = catalog::Manager::GetInstance().GetTileGroup(older_location.block);
    ContainerTuple<storage::TileGroup> older_tuple(older_tile_group.get(), older_location.offset);
    // remove the newer version from all the indexes
    // where it no longer matches the older version
    for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
      auto index = table->GetIndex(idx);
      if (index == nullptr) continue;
      auto index_schema = index->GetKeySchema();
      auto indexed_columns = index_schema->GetIndexedColumns();

      // build keys
      std::unique_ptr<storage::Tuple> older_key(new storage::Tuple(index_schema, true));
      older_key->SetFromTuple(&older_tuple, indexed_columns, index->GetPool());
      std::unique_ptr<storage::Tuple> newer_key(new storage::Tuple(index_schema, true));
      newer_key->SetFromTuple(&newer_tuple, indexed_columns, index->GetPool());

      // if newer_key is different, delete it from index
      if (newer_key->Compare(*older_key) != 0) {
        index->DeleteEntry(newer_key.get(), indirection);
      }
    }

  } else if (type == GCVersionType::TOMBSTONE) {
    // the gc'd version is a newly created empty version.
    // need to recycle this version.
    // no index manipulation needs to be made.
  } else {
    PELOTON_ASSERT(type == GCVersionType::ABORT_INSERT ||
              type == GCVersionType::COMMIT_INS_DEL ||
              type == GCVersionType::ABORT_INS_DEL ||
              type == GCVersionType::COMMIT_DELETE);

    // attempt to unlink the version from all the indexes.
    for (size_t idx = 0; idx < table->GetIndexCount(); ++idx) {
      auto index = table->GetIndex(idx);
      if (index == nullptr) continue;
      auto index_schema = index->GetKeySchema();
      auto indexed_columns = index_schema->GetIndexedColumns();

      // build key.
      std::unique_ptr<storage::Tuple> current_key(
          new storage::Tuple(index_schema, true));
      current_key->SetFromTuple(&current_tuple, indexed_columns,
                                index->GetPool());

      index->DeleteEntry(current_key.get(), indirection);
    }
  }
}

}  // namespace gc
}  // namespace peloton
