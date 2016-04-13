/*-------------------------------------------------------------------------
 *
 * logmanager.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/gc/gc_manager.cpp
 *
 *-------------------------------------------------------------------------
 */

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

GCManager::GCManager() { this->status = GC_STATUS_OFF; }

GCManager::~GCManager() {}

bool GCManager::GetStatus() { return this->status; }

void GCManager::SetStatus(GCStatus status) { this->status = status; }

// delete a tuple from all its indexes it belongs in
void GCManager::DeleteTupleFromIndexes(struct TupleMetadata tm) {
  auto &manager = catalog::Manager::GetInstance();
  auto db = manager.GetDatabaseWithOid(tm.database_id);
  auto table = db->GetTableWithOid(tm.table_id);
  auto index_count = table->GetIndexCount();
  auto tile_group = manager.GetTileGroup(tm.tile_group_id).get();
  auto tile_count = tile_group->GetTileCount();
  for (oid_t i = 0; i < tile_count; i++) {
    auto tile = tile_group->GetTile(i);
    for (oid_t j = 0; j < index_count; j++) {
      // delete tuple from each index
      auto index = table->GetIndex(j);
      ItemPointer item(tm.tile_group_id, tm.tuple_slot_id);
      auto index_schema = index->GetKeySchema();
      auto indexed_columns = index_schema->GetIndexedColumns();
      std::unique_ptr<storage::Tuple> key(
          new storage::Tuple(index_schema, true));
      char *tile_tuple_location = tile->GetTupleLocation(tm.tuple_slot_id);
      assert(tile_tuple_location);
      storage::Tuple tuple(tile->GetSchema(), tile_tuple_location);
      key->SetFromTuple(&tuple, indexed_columns, index->GetPool());
      index->DeleteEntry(key.get(), item);
    }
  }
}

void GCManager::Poll() {
  /*
   * Check if we can move anything from the possibly free list to the free
   * list.
   */
  auto &manager = catalog::Manager::GetInstance();

  while (1) {
    LOG_DEBUG("Polling GC thread...");

    int count = 0;
    auto &trans_mgr = concurrency::TransactionManagerFactory::GetInstance();
    auto oldest_trans = trans_mgr.GetMaxCommittedCid();
    // TODO:: calculating oldest_trans just once is conservative. We might get
    // better recycling by calculating oldest_trans more often, but calculating
    // it is expensive.

    while ((!possibly_free_list.empty()) && (count < MAX_TUPLES_PER_GC)) {
      count++;
      struct TupleMetadata tm;
      possibly_free_list.pop(tm);

      if (oldest_trans == INVALID_TXN_ID || oldest_trans == MAX_CID ||
          tm.transaction_id < oldest_trans) {
        // Now that we know we need to recycle tuple, we need to delete all
        // tuples from the indexes to which it belongs as well.
        DeleteTupleFromIndexes(tm);

        auto tile_group = manager.GetTileGroup(tm.tile_group_id).get();
        tile_group->GetHeader()->SetTransactionId(tm.tuple_slot_id,
                                                  INVALID_TXN_ID);
        tile_group->GetHeader()->SetBeginCommitId(tm.tuple_slot_id, MAX_CID);
        tile_group->GetHeader()->SetEndCommitId(tm.tuple_slot_id, MAX_CID);

        std::string key =
            std::to_string(tm.database_id) + std::to_string(tm.table_id);
        boost::lockfree::queue<struct TupleMetadata> *free_list = nullptr;

        // we can now put the possibly free tuple into the actually free list
        if (free_map.find(key, free_list)) {
          free_list->push(tm);
        } else {
          free_list = new boost::lockfree::queue<struct TupleMetadata>(
              MAX_TUPLES_PER_GC);
          free_list->push(tm);
          free_map.insert(key, free_list);
        }
      } else {
        // if a tuple can't be reaped, add it back to the list.
        possibly_free_list.push(tm);
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
}

// this function returns a free tuple slot, if one exists
oid_t GCManager::ReturnFreeSlot(oid_t db_id, oid_t tb_id) {
  auto return_slot = INVALID_OID;
  std::string key = std::to_string(db_id) + std::to_string(tb_id);
  boost::lockfree::queue<struct TupleMetadata> *free_list = nullptr;
  if (free_map.find(key, free_list)) {
    if (!free_list->empty()) {
      struct TupleMetadata tm;
      free_list->pop(tm);
      return_slot = tm.tuple_slot_id;
      return return_slot;
    }
  }
  return INVALID_OID;
}

// this function adds a tuple to the possibly free list
void GCManager::AddPossiblyFreeTuple(struct TupleMetadata tm) {
  this->possibly_free_list.push(tm);
}

}  // namespace gc
}  // namespace peloton
