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

GCManager::GCManager() {
  this->status = GC_STATUS_OFF;
}

GCManager::~GCManager() {}

bool GCManager::GetStatus() {
  return this->status;
}

void GCManager::SetStatus(GCStatus status) {
  this->status = status;
}

void GCManager::DeleteTupleFromIndexes(struct TupleMetadata tm) {
  auto &manager = catalog::Manager::GetInstance();
  auto db = manager.GetDatabaseWithOid(tm.database_id);
  auto table = db->GetTableWithOid(tm.table_id);
  auto index_count = table->GetIndexCount();
  auto tile_group = manager.GetTileGroup(tm.tile_group_id).get();
  auto tile_count = tile_group->GetTileCount();
  for(oid_t i=0; i<tile_count; i++) {
    auto tile = tile_group->GetTile(i);
    for(oid_t j=0; j<index_count; j++) {
      // delete tuple from each index
      auto index = table->GetIndex(j);
      ItemPointer item(tm.tile_group_id, tm.tuple_slot_id);
      auto index_schema = index->GetKeySchema();
      auto indexed_columns = index_schema->GetIndexedColumns();
      std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
      char *tile_tuple_location = tile->GetTupleLocation(tm.tuple_slot_id);
      assert(tile_tuple_location);
      storage::Tuple tuple(tile->GetSchema(), tile_tuple_location);
      key->SetFromTuple(&tuple, indexed_columns, index->GetPool());
      index->DeleteEntry(key.get(), item);
    }
  }
}

void GCManager::Poll() {
  LOG_DEBUG("Polling GC thread...");
  /*
   * Check if we can move anything from the possibly free list to the free
   * list.
   */
  {
    // std::lock_guard<std::mutex> lock(gc_mutex);
    if(!possibly_free_list.empty()) {
      auto &trans_mgr = concurrency::TransactionManagerFactory::GetInstance();
      auto oldest_trans = trans_mgr.GetMaxCommittedCid();

      // for(auto it=possibly_free_list.begin(); it != possibly_free_list.end(); ) {
        struct TupleMetadata tm;
        possibly_free_list.pop(tm);
        if(oldest_trans == INVALID_TXN_ID || tm.transaction_id < oldest_trans) {
          /*
           * Now that we know we need to recycle tuple, we need to delete all
           * tuples from the indexes to which it belongs as well.
           */
          DeleteTupleFromIndexes(tm);

          auto free_map_it = free_map.find(std::pair<oid_t, oid_t>(tm.database_id, tm.table_id));
          if(free_map_it != free_map.end()) {
            /*boost::lockfree::queue<struct TupleMetadata> *free_list = new boost::lockfree::queue<struct TupleMetadata>(100);
            struct TupleMetadata temp_tm;
            while(free_map_it->second->pop(temp_tm)) {
              free_list -> push(temp_tm);
            }
            free_list->push(tm);
            std::pair<oid_t, oid_t> key(tm.database_id, tm.table_id);
            std::pair<std::pair<oid_t, oid_t>, boost::lockfree::queue<struct TupleMetadata>*> p(key, free_list);
            free_map.erase(key);
            free_map.insert(p);*/

            // we need to create list
            free_map_it->second -> push(tm);
            // free_list = free_map_it->second;
            // free_map.erase(free_map_it);
          } else {
            boost::lockfree::queue<struct TupleMetadata> *free_list = new boost::lockfree::queue<struct TupleMetadata>(100);
            free_list -> push(tm);
            std::pair<oid_t, oid_t> key(tm.database_id, tm.table_id);
            std::pair<std::pair<oid_t, oid_t>, boost::lockfree::queue<struct TupleMetadata>*> p(key, free_list);
            free_map.insert(p);
          }
          LOG_INFO("adding tuple to be recycled");
          // free_list.push_back(tm);
          // LOG_INFO("The free_list size is %lu", free_list.size());
          //
          // free_map[key] = free_list;


          /*if(free_map_it != free_map.end()) {
            LOG_INFO("Recycling inside Poll function");
            auto free_list = free_map_it->second;
            free_list.push_back(tm);
            LOG_INFO("The free_list size is %lu", free_list.size());
            free_map.erase(free_map_it);
            std::pair<oid_t, oid_t> key(tm.database_id, tm.table_id);
            free_map[key] = free_list;
            } else {
            std::deque<struct TupleMetadata> free_list;
            free_list.push_back(tm);
            std::pair<oid_t, oid_t> key(tm.database_id, tm.table_id);
            free_map[key] = free_list;
            }*/
          // it = possibly_free_list.erase(it);
        } else {
          LOG_INFO("oldest transaction %lu", oldest_trans);
          possibly_free_list.push(tm);
        }
      // }
    }
  }

  std::this_thread::sleep_for(std::chrono::seconds(30));
  Poll();
}

oid_t GCManager::ReturnFreeSlot(oid_t db_id, oid_t tb_id) {
  auto return_slot = INVALID_OID;
  // {
    // std::lock_guard<std::mutex> lock(gc_mutex);
    auto free_map_it = free_map.find(std::pair<oid_t, oid_t>(db_id, tb_id));
    if(free_map_it != free_map.end()) {
      if(!(free_map_it->second)->empty()) {
        struct TupleMetadata tm ;
        LOG_INFO("using recycled tuple");
        (free_map_it->second)->pop(tm);
        return_slot = tm.tuple_slot_id;
        return return_slot;
      }
    }
  // }
  return INVALID_OID;
}

void GCManager::AddPossiblyFreeTuple(struct TupleMetadata tm) {
  {
    // std::lock_guard<std::mutex> lock(gc_mutex);
    printf("possible free list addition\n");
    this->possibly_free_list.push(tm);
  }
}

}  // namespace gc
}  // namespace peloton
