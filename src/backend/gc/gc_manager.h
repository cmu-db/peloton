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

struct GCContext {
  GCContext() : possibly_free_list_(MAX_FREE_LIST_LENGTH) {
    is_running_ = true;
  }
  volatile bool is_running_;
  cuckoohash_map<oid_t, LockfreeQueue<TupleMetadata>*> free_map_;
  LockfreeQueue<TupleMetadata> possibly_free_list_;
};

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
  static GCManager &GetInstance();

  void Poll(GCContext *);

  // Get status of whether GC thread is running or not
  bool GetStatus() { return this->status_; }

  void SetStatus(const GCStatus &status) { this->status_ = status; }

  void StartGC(const oid_t &database_id);

  void StopGC(const oid_t &database_id);

  void AddPossiblyFreeTuple(const oid_t &database_id, const TupleMetadata &);

  ItemPointer ReturnFreeSlot(const oid_t &database_id, const oid_t &table_id);

 private:
  GCManager() { 
    this->status_ = GC_STATUS_OFF;
  }

  ~GCManager() {}

  void DeleteTupleFromIndexes(const TupleMetadata&);

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  GCStatus status_;
  cuckoohash_map<oid_t, std::pair<std::thread*, GCContext*>> gc_contexts_;


};

}  // namespace gc
}  // namespace peloton
