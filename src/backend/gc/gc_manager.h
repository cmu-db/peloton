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

#include "backend/common/types.h"
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

  virtual void RecycleTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                        const oid_t &tuple_id, const cid_t &tuple_end_cid) = 0;

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id) = 0;
};

}  // namespace gc
}  // namespace peloton
