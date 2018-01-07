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

#include <memory>
#include <thread>
#include <vector>

#include "common/item_pointer.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/internal_types.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace storage {
class TileGroup;
}

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

  GCManager() : is_running_(false) {}

  virtual ~GCManager() {}

  static GCManager &GetInstance() {
    static GCManager gc_manager;
    return gc_manager;
  }

  virtual void Reset() { is_running_ = false; }

  // Get status of whether GC thread is running or not
  bool GetStatus() { return this->is_running_; }

  virtual void StartGC(
      std::vector<std::unique_ptr<std::thread>> &UNUSED_ATTRIBUTE) {}

  virtual void StartGC() {}

  virtual void StopGC() {}

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id UNUSED_ATTRIBUTE) {
    return INVALID_ITEMPOINTER;
  }

  virtual void RegisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual void DeregisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual size_t GetTableCount() { return 0; }

  virtual void RecycleTransaction(
                      concurrency::TransactionContext *txn UNUSED_ATTRIBUTE) {}

 protected:
  void CheckAndReclaimVarlenColumns(storage::TileGroup *tg, oid_t tuple_id);

 protected:
  volatile bool is_running_;
};

}  // namespace gc
}  // namespace peloton
