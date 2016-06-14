//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// vacuum_gc.h
//
// Identification: src/backend/gc/vacuum_gc.h
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
class Vacuum_GCManager : public GCManager {
public:
  Vacuum_GCManager(int thread_count)
    : is_running_(true),
      gc_thread_count_(thread_count),
      gc_threads_(thread_count),
      unlink_queues_(),
      reclaim_maps_(thread_count){
    for (int i = 0; i < gc_thread_count_; ++i) {
      std::shared_ptr<LockfreeQueue<TupleMetadata>> unlink_queue(new LockfreeQueue<TupleMetadata>(MAX_QUEUE_LENGTH));
      unlink_queues_.push_back(unlink_queue);
    }
    StartGC();
  }

  ~Vacuum_GCManager() { StopGC(); }

  static Vacuum_GCManager& GetInstance(int thread_count = 1) {
    static Vacuum_GCManager gcManager(thread_count);
    return gcManager;
  }

  // Get status of whether GC thread is running or not
  virtual bool GetStatus() { return this->is_running_; }

  virtual void StartGC() {
    for (int i = 0; i < gc_thread_count_; ++i) {
      StartGC(i);
    }
  };

  virtual void StopGC() {
    for (int i = 0; i < gc_thread_count_; ++i) {
      StopGC(i);
    }
  };

  virtual void RecycleOldTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                                const oid_t &tuple_id, const cid_t &tuple_end_cid) {
    RecycleOldTupleSlot(table_id, tile_group_id, tuple_id, tuple_end_cid, HashToThread(tuple_id));
  }

  virtual void RecycleInvalidTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id);

  virtual void RegisterTable(oid_t table_id) {
    // Insert a new entry for the table
    if (recycle_queue_map_.find(table_id) == recycle_queue_map_.end()) {
      LOG_TRACE("register table %d to garbage collector", (int)table_id);
      std::shared_ptr<LockfreeQueue<TupleMetadata>> recycle_queue(new LockfreeQueue<TupleMetadata>(MAX_QUEUE_LENGTH));
      recycle_queue_map_[table_id] = recycle_queue;
    }
  }

private:
  void StartGC(int thread_id);

  void StopGC(int thread_id);

  void RecycleOldTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                                   const oid_t &tuple_id, const cid_t &tuple_end_cid, int thread_id);

  inline int HashToThread(const oid_t &tuple_id) {
    return tuple_id % gc_thread_count_;
  }

  void ClearGarbage(int thread_id);

  void Running(int thread_id);

  void Reclaim(int thread_id, const cid_t &max_cid);

  void Unlink(int thread_id, const cid_t &max_cid);

  void DeleteTupleFromIndexes(const TupleMetadata &);
  
  void AddToRecycleMap(const TupleMetadata &tuple_metadata);

  bool ResetTuple(const TupleMetadata &);

private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  volatile bool is_running_;

  const int gc_thread_count_;

  std::vector<std::unique_ptr<std::thread>> gc_threads_;

  std::vector<std::shared_ptr<LockfreeQueue<TupleMetadata>>> unlink_queues_;

  // Map of actual grabage.
  // The key is the timestamp when the garbage is identified, value is the
  // metadata of the garbage.
  // TODO: use shared pointer to reduce memory copy
  std::vector<std::multimap<cid_t, TupleMetadata>> reclaim_maps_;

  // TODO: use shared pointer to reduce memory copy
  // table_id -> queue
  //cuckoohash_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;
  std::unordered_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;
};
}
}

