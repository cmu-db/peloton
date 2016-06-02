//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager.h
//
// Identification: src/gc/gc_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <unordered_map>
#include <map>
#include <vector>

#include "common/types.h"
#include "common/lockfree_queue.h"
#include "common/logger.h"
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
  ~GCBuffer();
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

  GCManager(const GCType type)
      : is_running_(true),
        gc_type_(type),
        reclaim_queue_(MAX_QUEUE_LENGTH) {
    StartGC();
  }

  ~GCManager() { StopGC(); }

  // Get status of whether GC thread is running or not
  bool GetStatus() { return this->is_running_; }

  void StartGC();

  void StopGC();

  void RecycleTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                        const oid_t &tuple_id, const cid_t &tuple_end_cid);

  ItemPointer ReturnFreeSlot(const oid_t &table_id);

 private:
  void Running();

  bool ResetTuple(const TupleMetadata &);

 private:
  //===--------------------------------------------------------------------===//
  // Private methods
  //===--------------------------------------------------------------------===//
  void ClearGarbage();

  void AddToRecycleMap(TupleMetadata tuple_metadata);

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  volatile bool is_running_;
  GCType gc_type_;

  std::unique_ptr<std::thread> gc_thread_;

  // TODO: use shared pointer to reduce memory copy
  LockfreeQueue<TupleMetadata> reclaim_queue_;

  // TODO: use shared pointer to reduce memory copy
  cuckoohash_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>>
      recycle_queue_map_;
};

}  // namespace gc
}  // namespace peloton
