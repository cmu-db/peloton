//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// n2o_gc.h
//
// Identification: src/backend/gc/n2o_gc.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <unordered_map>
#include <map>
#include <vector>

#include "backend/common/types.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "backend/gc/gc_manager.h"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

class N2O_GCManager : public GCManager {
public:
  N2O_GCManager()
    : is_running_(true),
      reclaim_queue_(MAX_QUEUE_LENGTH) {
    StartGC();
  }

  virtual ~N2O_GCManager() { StopGC(); }

  static N2O_GCManager &GetInstance() {
    static N2O_GCManager gcManager;
    return gcManager;
  }

  // Get status of whether GC thread is running or not
  virtual bool GetStatus() { return this->is_running_; }

  virtual void StartGC();

  virtual void StopGC();


  virtual void RecycleOldTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                        const oid_t &tuple_id, const cid_t &tuple_end_cid);


  virtual void RecycleInvalidTupleSlot(const oid_t &table_id, const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id);

  void RegisterTable(oid_t table_id) {
    // Insert a new entry for the table
    if (recycle_queue_map_.find(table_id) == recycle_queue_map_.end()) {
      LOG_TRACE("register table %d to garbage collector", (int)table_id);
      std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue(new LockfreeQueue<TupleMetadata>(MAX_QUEUE_LENGTH));
      recycle_queue_map_[table_id] = recycle_queue;
    }
  }

private:
  void Running();

  bool ResetTuple(const TupleMetadata &);

private:
  //===--------------------------------------------------------------------===//
  // Private methods
  //===--------------------------------------------------------------------===//
  void ClearGarbage();

  void AddToRecycleMap(const TupleMetadata &tuple_metadata);

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  volatile bool is_running_;

  std::unique_ptr<std::thread> gc_thread_;

  // TODO: use shared pointer to reduce memory copy
  LockfreeQueue<TupleMetadata> reclaim_queue_;

  // TODO: use shared pointer to reduce memory copy
  // cuckoohash_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;

  std::unordered_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;
  //std::unordered_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;
};

}  // namespace gc
}  // namespace peloton
