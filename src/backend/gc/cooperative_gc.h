//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager.h
//
// Identification: src/backend/gc/cooperative_gc.h
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
#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

class Cooperative_GCManager : public GCManager {
public:
  Cooperative_GCManager()
    : is_running_(true),
      reclaim_queue_(MAX_QUEUE_LENGTH),
      recycle_queue_(MAX_QUEUE_LENGTH) {
    StartGC();
  }

  virtual ~Cooperative_GCManager() { StopGC(); }

  static Cooperative_GCManager &GetInstance() {
    static Cooperative_GCManager gcManager;
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

private:
  void Running();
  //void DeleteTupleFromIndexes(const TupleMetadata &);

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
  //cuckoohash_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;
  LockfreeQueue<TupleMetadata> recycle_queue_;
};

}  // namespace gc
}  // namespace peloton
