//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager.h
//
// Identification: src/include/gc/gc_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "common/macros.h"
#include "common/types.h"
#include "common/logger.h"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

class GCManager {
 public:
  GCManager(const GCManager &) = delete;
  GCManager &operator=(const GCManager &) = delete;
  GCManager(GCManager &&) = delete;
  GCManager &operator=(GCManager &&) = delete;

  GCManager() : is_running_(true) { StartGC(); }

  ~GCManager() { StopGC(); }

  static GCManager& GetInstance() {
    static GCManager gc_manager;
    return gc_manager;
  }

  // Get status of whether GC thread is running or not
  bool GetStatus() { return this->is_running_; }

  void StartGC() {}

  void StopGC() {}

  void RecycleTupleSlot(const oid_t &table_id UNUSED_ATTRIBUTE, 
                        const ItemPointer &item_pointer UNUSED_ATTRIBUTE) {}

  ItemPointer ReturnFreeSlot(const oid_t &table_id UNUSED_ATTRIBUTE) {
    return INVALID_ITEMPOINTER;
  }

  virtual void RegisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {

  }

  virtual void RegisterCommittedTransaction(const RWSet &rw_set UNUSED_ATTRIBUTE, const cid_t &timestamp UNUSED_ATTRIBUTE) {}

  virtual void RegisterAbortedTransaction(const RWSet &rw_set UNUSED_ATTRIBUTE, const cid_t &timestamp UNUSED_ATTRIBUTE) {}

 private:
  bool is_running_;
};

}  // namespace gc
}  // namespace peloton
