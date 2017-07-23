//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_level_gc_manager.h
//
// Identification: src/include/gc/transaction_level_gc_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <unordered_map>
#include <map>
#include <vector>
#include <list>

#include "type/types.h"
#include "common/logger.h"
#include "common/init.h"
#include "common/thread_pool.h"
#include "gc/gc_manager.h"

#include "container/lock_free_queue.h"

namespace peloton {
namespace gc {

#define MAX_QUEUE_LENGTH 100000
#define MAX_ATTEMPT_COUNT 100000


struct GarbageContext {
  GarbageContext() : epoch_id_(INVALID_EID) {}
  GarbageContext(std::shared_ptr<GCSet> gc_set, 
                 const eid_t &epoch_id) {
    gc_set_ = gc_set;
    epoch_id_ = epoch_id;
  }

  std::shared_ptr<GCSet> gc_set_;
  eid_t epoch_id_;
};

class TransactionLevelGCManager : public GCManager {
public:
  TransactionLevelGCManager(const int thread_count) 
    : gc_thread_count_(thread_count),
      reclaim_maps_(thread_count) {

    unlink_queues_.reserve(thread_count);
    for (int i = 0; i < gc_thread_count_; ++i) {
      std::shared_ptr<LockFreeQueue<std::shared_ptr<GarbageContext>>> unlink_queue(
        new LockFreeQueue<std::shared_ptr<GarbageContext>>(MAX_QUEUE_LENGTH)
      );
      unlink_queues_.push_back(unlink_queue);
      local_unlink_queues_.emplace_back();
    }
  }

  virtual ~TransactionLevelGCManager() { }

  static TransactionLevelGCManager& GetInstance(const int thread_count = 1) {
    static TransactionLevelGCManager gc_manager(thread_count);
    return gc_manager;
  }

  virtual void StartGC(std::vector<std::unique_ptr<std::thread>> &gc_threads) override {
    LOG_TRACE("Starting GC");
    this->is_running_ = true;
    gc_threads.resize(gc_thread_count_);
    for (int i = 0; i < gc_thread_count_; ++i) {
      gc_threads[i].reset(new std::thread(&TransactionLevelGCManager::Running, this, i));

    }
  }

  virtual void StartGC() override {
    LOG_TRACE("Starting GC");
    this->is_running_ = true;
    for (int i = 0; i < gc_thread_count_; ++i) {
      thread_pool.SubmitDedicatedTask(&TransactionLevelGCManager::Running, this, std::move(i));
    }
  };

  virtual void StopGC() override {
    LOG_TRACE("Stopping GC");
    this->is_running_ = false;
  }

  virtual void RecycleTransaction(std::shared_ptr<GCSet> gc_set, const eid_t &epoch_id, const size_t &thread_id) override;

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id) override;

  virtual void RegisterTable(const oid_t &table_id) override {
    // Insert a new entry for the table
    if (recycle_queue_map_.find(table_id) == recycle_queue_map_.end()) {
      std::shared_ptr<LockFreeQueue<ItemPointer>> recycle_queue(new LockFreeQueue<ItemPointer>(MAX_QUEUE_LENGTH));
      recycle_queue_map_[table_id] = recycle_queue;
    }
  }

  virtual void DeregisterTable(const oid_t &table_id) override {
    // Remove dropped tables
    if (recycle_queue_map_.find(table_id) != recycle_queue_map_.end()) {
      recycle_queue_map_.erase(table_id);
    }
  }

  virtual size_t GetTableCount() override {
    return recycle_queue_map_.size();
  }

  int Unlink(const int &thread_id, const eid_t &expired_eid);

  int Reclaim(const int &thread_id, const eid_t &expired_eid);

private:

  inline unsigned int HashToThread(const size_t &thread_id) {
    return (unsigned int)thread_id % gc_thread_count_;
  }

  void ClearGarbage(int thread_id);

  void Running(const int &thread_id);

  void AddToRecycleMap(std::shared_ptr<GarbageContext> gc_ctx);

  bool ResetTuple(const ItemPointer &);

  void UnlinkVersions(const std::shared_ptr<GarbageContext>& garbage_ctx);

  void UnlinkVersion(const ItemPointer location, const GCVersionType type);

private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  int gc_thread_count_;

  // queues for to-be-unlinked tuples.
  // # unlink_queues == # gc_threads
  std::vector<std::shared_ptr<peloton::LockFreeQueue<std::shared_ptr<GarbageContext>>>> unlink_queues_;
  
  // local queues for to-be-unlinked tuples.
  // # local_unlink_queues == # gc_threads
  std::vector<std::list<std::shared_ptr<GarbageContext>>> local_unlink_queues_;

  // multimaps for to-be-reclaimed tuples.
  // The key is the timestamp when the garbage is identified, value is the
  // metadata of the garbage.
  // # reclaim_maps == # gc_threads
  std::vector<std::multimap<cid_t, std::shared_ptr<GarbageContext>>> reclaim_maps_;

  // queues for to-be-reused tuples.
  // # recycle_queue_maps == # tables
  std::unordered_map<oid_t, std::shared_ptr<peloton::LockFreeQueue<ItemPointer>>> recycle_queue_map_;

};
}
}

