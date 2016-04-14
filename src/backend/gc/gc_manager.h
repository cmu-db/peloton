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

#include <mutex>
#include <thread>
#include <deque>
#include <map>
#include <list>

#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

#define MAX_TUPLES_PER_GC 1000
#define MAX_FREE_LIST_LENGTH 1000

/**
 * Global GC Manager
 */
class GCManager {
 public:
  GCManager(const GCManager &) = delete;
  GCManager &operator=(const GCManager &) = delete;
  GCManager(GCManager &&) = delete;
  GCManager &operator=(GCManager &&) = delete;

  // global singleton
  static GCManager &GetInstance(void);

  void Poll();

  // Get status of whether GC thread is running or not
  bool GetStatus() { return this->status; }

  void SetStatus(const GCStatus &status) { this->status = status; }


  void AddPossiblyFreeTuple(const TupleMetadata &);

  oid_t ReturnFreeSlot(const oid_t &database_id, const oid_t &table_id);

 private:
  GCManager() : possibly_free_list_(MAX_FREE_LIST_LENGTH) { this->status = GC_STATUS_OFF; }

  ~GCManager() {}

  void DeleteTupleFromIndexes(struct TupleMetadata tm);


 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  GCStatus status;
  cuckoohash_map<oid_t, LockfreeQueue<TupleMetadata>*> free_map_;
  LockfreeQueue<TupleMetadata> possibly_free_list_;

};

}  // namespace gc
}  // namespace peloton
