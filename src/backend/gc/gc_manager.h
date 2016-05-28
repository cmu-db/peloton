//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager.h
//
// Identification: src/backend/gc/gc_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <unordered_map>
#include <map>
#include <vector>
#include "backend/storage/table_factory.h"
#include "backend/storage/tuple.h"
#include "backend/index/index.h"
#include "backend/common/types.h"
#include "backend/storage/tile_group_factory.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

#define MAX_ATTEMPT_COUNT 100000
#define MAX_QUEUE_LENGTH 100000

#define GC_PERIOD_MILLISECONDS 100
class GCBuffer {
public:
  GCBuffer(oid_t tid):table_id(tid), garbage_tuples() {}
  virtual ~GCBuffer();
  inline void AddGarbage(const ItemPointer& itemptr) {garbage_tuples.push_back(itemptr);}
private:
  oid_t table_id;
  std::vector<ItemPointer> garbage_tuples;
};

class GCManager {
 public:
  GCManager(const GCManager &) = delete;
  GCManager &operator=(const GCManager &) = delete;
  GCManager(GCManager &&) = delete;
  GCManager &operator=(GCManager &&) = delete;

  GCManager() {}

  virtual ~GCManager() {};

  // Get status of whether GC thread is running or not
  virtual bool GetStatus() = 0;

  virtual void StartGC() = 0;

  virtual void StopGC() = 0;

  // recycle old version
  virtual void RecycleOldTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                        const oid_t &tuple_id, const cid_t &tuple_end_cid) = 0;

  // recycle invalid version
  virtual void RecycleInvalidTupleSlot(const oid_t &table_id, const oid_t &tile_group_id, const oid_t &tuple_id) = 0;

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id) = 0;

protected:
  void DeleteInvalidTupleFromIndex(const TupleMetadata __attribute__((unused))&tuple_metadata) {
    return;
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tuple_metadata.tile_group_id);
    LOG_TRACE("Deleting index for tuple(%u, %u)", tuple_metadata.tile_group_id,
             tuple_metadata.tuple_slot_id);

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
        new storage::Tuple(index_schema, true));
      key->SetFromTuple(expired_tuple.get(), indexed_columns, index->GetPool());

      // todo: if invalid version is insert, still need to remove from primary index
      if(index->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
        continue;
      }

      index->DeleteEntry(key.get(),
                         ItemPointer(tuple_metadata.tile_group_id,
                                     tuple_metadata.tuple_slot_id));
    }
  }
};

}  // namespace gc
}  // namespace peloton