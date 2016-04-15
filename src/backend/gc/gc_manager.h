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
  static GCManager &GetInstance();

  void Poll();

  // Get status of whether GC thread is running or not
  bool GetStatus() { return this->status_; }

  void SetStatus(const GCStatus &status) { this->status_ = status; }


  void AddPossiblyFreeTuple(const TupleMetadata &);

  oid_t ReturnFreeSlot(const oid_t &database_id, const oid_t &table_id);

 private:
  GCManager() : possibly_free_list_(MAX_FREE_LIST_LENGTH) { this->status_ = GC_STATUS_OFF; }

  ~GCManager() {}

  //void DeleteTupleFromIndexes(TupleMetadata tm);

#define DATABASE_ID_MASK 0xFFFFFFFF
#define TABLE_ID_MASK 0x00000000FFFFFFFF
  inline oid_t PACK_ID(oid_t database_id, oid_t table_id) {
    return ((long)(database_id & DATABASE_ID_MASK) << 32) | (table_id & TABLE_ID_MASK);
  }


 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  GCStatus status_;
  cuckoohash_map<oid_t, LockfreeQueue<TupleMetadata>*> free_map_;
  LockfreeQueue<TupleMetadata> possibly_free_list_;

};

}  // namespace gc
}  // namespace peloton
