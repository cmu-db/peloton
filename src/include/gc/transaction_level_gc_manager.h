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

#include "common/types.h"
#include "common/logger.h"
#include "gc/gc_manager.h"
#include "concurrency/transaction.h"

#include "container/lock_free_queue.h"

namespace peloton {
namespace gc {

#define MAX_QUEUE_LENGTH 100000
#define MAX_ATTEMPT_COUNT 100000

struct GarbageContext {
  GarbageContext() : timestamp_(INVALID_CID) {}
  GarbageContext(const RWSet &rw_set, const cid_t &timestamp) : rw_set_(rw_set), timestamp_(timestamp) {}

  RWSet rw_set_;
  cid_t timestamp_;
};

class TransactionLevelGCManager : public GCManager {
public:
  TransactionLevelGCManager(int thread_count) 
    : is_running_(true),
      gc_thread_count_(thread_count),
      gc_threads_(thread_count),
      reclaim_maps_(thread_count) {

    unlink_queues_.reserve(thread_count);
    for (int i = 0; i < gc_thread_count_; ++i) {
      std::shared_ptr<LockFreeQueue<std::shared_ptr<GarbageContext>>> unlink_queue(
        new LockFreeQueue<std::shared_ptr<GarbageContext>>(MAX_QUEUE_LENGTH)
      );
      unlink_queues_.push_back(unlink_queue);
      local_unlink_queues_.emplace_back();
    }

    StartGC(); 
  }

  ~TransactionLevelGCManager() { StopGC(); }

  static TransactionLevelGCManager& GetInstance(int thread_count = 1) {
    static TransactionLevelGCManager gc_manager(thread_count);
    return gc_manager;
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
  }

  void RegisterTransaction(const RWSet &rw_set, const cid_t &timestamp);

  // void RegisterAbortedTransaction(const RWSet &rw_set, const cid_t &timestamp);

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id);

  virtual void RegisterTable(const oid_t table_id) {
    // Insert a new entry for the table
    if (recycle_queue_map_.find(table_id) == recycle_queue_map_.end()) {
      LOG_TRACE("register table %d to garbage collector", (int)table_id);
      std::shared_ptr<LockFreeQueue<ItemPointer>> recycle_queue(new LockFreeQueue<ItemPointer>(MAX_QUEUE_LENGTH));
      recycle_queue_map_[table_id] = recycle_queue;
    }
  }

private:
  void StartGC(int thread_id);

  void StopGC(int thread_id);

  inline unsigned int HashToThread(const cid_t &ts) {
    return (unsigned int)ts % gc_thread_count_;
  }

  void ClearGarbage(int thread_id);

  void Running(const int &thread_id);

  void Unlink(const int &thread_id, const cid_t &max_cid);

  void Reclaim(const int &thread_id, const cid_t &max_cid);

  void AddToRecycleMap(std::shared_ptr<GarbageContext> gc_ctx);

  bool ResetTuple(const ItemPointer &);

  void DeleteTupleFromIndexes(const ItemPointer &tuple_metadata);

private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  volatile bool is_running_;

  int gc_thread_count_;

  std::vector<std::unique_ptr<std::thread>> gc_threads_;

  // queues for to-be-unlinked tuples.
  std::vector<std::shared_ptr<peloton::LockFreeQueue<std::shared_ptr<GarbageContext>>>> unlink_queues_;
  
  // local queues for to-be-unlinked tuples.
  std::vector<std::list<std::shared_ptr<GarbageContext>>> local_unlink_queues_;

  // multimaps for to-be-reclaimed tuples.
  // The key is the timestamp when the garbage is identified, value is the
  // metadata of the garbage.
  std::vector<std::multimap<cid_t, std::shared_ptr<GarbageContext>>> reclaim_maps_;

  // queues for to-be-reused tuples.
  std::unordered_map<oid_t, std::shared_ptr<peloton::LockFreeQueue<ItemPointer>>> recycle_queue_map_;

};
}
}

