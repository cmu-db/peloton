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

static std::atomic<uint64_t> possibly_free_count(0);

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

GCManager::~GCManager() {
  LOG_INFO("Destructor is caled");
}

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
  /*
   * Check if we can move anything from the possibly free list to the free
   * list.
   */
  while(1) {
    LOG_INFO("Polling GC thread...");

    int count = 0;
    LOG_INFO("possibly free count = %lu", possibly_free_count.load());

    while((!possibly_free_list.empty()) && (count < 10)) {
      LOG_INFO("line 1");
      count++;
//#if 0
      auto &trans_mgr = concurrency::TransactionManagerFactory::GetInstance();
      LOG_INFO("line 2");

      auto oldest_trans = trans_mgr.GetMaxCommittedCid();
      LOG_INFO("oldest trans = %lu", oldest_trans);
      struct TupleMetadata tm;
      LOG_INFO("line 3");
      possibly_free_list.pop(tm);
      LOG_INFO("line 4");

      if(oldest_trans == INVALID_TXN_ID || oldest_trans == MAX_CID || tm.transaction_id < oldest_trans) {
        /*
         * Now that we know we need to recycle tuple, we need to delete all
         * tuples from the indexes to which it belongs as well.
         */
        LOG_INFO("line 5");
        DeleteTupleFromIndexes(tm);
        LOG_INFO("line 6");

        LOG_INFO("TM details are: tm.database_id = %lu, tm.table_id = %lu, tm.tile_group_id = %lu, tm.tuple_slot_id = %lu", tm.database_id, tm.table_id, tm.tile_group_id, tm.tuple_slot_id);
        actual_free_list.push(tm);

#if 0
        std::string key = std::to_string(tm.database_id) + std::to_string(tm.table_id);
        boost::lockfree::queue<struct TupleMetadata> *free_list = nullptr;
        LOG_INFO("before find");
        {
          std::lock_guard<std::mutex> lock(gc_mutex);

          //if(free_map.find(key, free_list)) {
          auto free_map_it = free_map.find(key);
          if(free_map_it != free_map.end()) {
            // we need to create list
            LOG_INFO("before push");
            free_list = free_map_it->second;
            free_list->push(tm);
            LOG_INFO("after push");
          } else {
            free_list = new boost::lockfree::queue<struct TupleMetadata>(100);
            LOG_INFO("before new push");
            free_list -> push(tm);
            LOG_INFO("after new push");
            //free_map.insert(key, free_list);
            //free_map[key] = free_list;
            free_map.insert(std::pair<std::string, boost::lockfree::queue<struct TupleMetadata> *>(key, free_list));
            LOG_INFO("after new insert");
          }
        }
#endif
        } else {
          LOG_INFO("oldest transaction %lu", oldest_trans);
          possibly_free_list.push(tm);
        }

        LOG_INFO("line 7");
      }

      LOG_INFO("just before sleeping");
      std::this_thread::sleep_for(std::chrono::seconds(10));
      LOG_INFO("just after sleeping");
      //Poll();
    }
}

oid_t GCManager::ReturnFreeSlot(oid_t db_id __attribute__((unused)), oid_t tb_id __attribute__((unused))) {
  if(!actual_free_list.empty()) {
    struct TupleMetadata tm ;
    LOG_INFO("using recycled tuple");
    actual_free_list.pop(tm);
    return tm.tuple_slot_id;
  }
#if 0
  auto return_slot = INVALID_OID;
  {
    std::lock_guard<std::mutex> lock(gc_mutex);
    std::string key = std::to_string(db_id) + std::to_string(tb_id);
    //auto free_map_it = free_map.find(std::pair<oid_t, oid_t>(db_id, tb_id));
    boost::lockfree::queue<struct TupleMetadata> *free_list = nullptr;
    //if(free_map.find(key, free_list)) {
    auto free_map_it = free_map.find(key);
    if(free_map_it != free_map.end()) {
      free_list = free_map_it->second;
      if(!free_list->empty()) {
        struct TupleMetadata tm ;
        LOG_INFO("using recycled tuple");
        free_list->pop(tm);
        return_slot = tm.tuple_slot_id;
        return return_slot;
      }
    }
  }
#endif
  return INVALID_OID;
}

void GCManager::AddPossiblyFreeTuple(struct TupleMetadata tm) {
  {
    // std::lock_guard<std::mutex> lock(gc_mutex);
    //printf("possible free list addition\n");
    possibly_free_count.fetch_add(1);
    this->possibly_free_list.push(tm);
  }
}

}  // namespace gc
}  // namespace peloton
