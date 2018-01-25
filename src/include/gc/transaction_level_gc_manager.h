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

#include <list>
#include <map>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/init.h"
#include "common/logger.h"
#include "common/thread_pool.h"
#include "concurrency/transaction_context.h"
#include "gc/gc_manager.h"
#include "common/internal_types.h"

#include "common/container/lock_free_queue.h"

namespace peloton {
namespace gc {

#define MAX_QUEUE_LENGTH 100000
#define MAX_ATTEMPT_COUNT 100000

class TransactionLevelGCManager : public GCManager {
 public:
  TransactionLevelGCManager(const int thread_count)
      : gc_thread_count_(thread_count), reclaim_maps_(thread_count) {
    unlink_queues_.reserve(thread_count);
    for (int i = 0; i < gc_thread_count_; ++i) {
      std::shared_ptr<LockFreeQueue<concurrency::TransactionContext* >>
          unlink_queue(new LockFreeQueue<concurrency::TransactionContext* >(
              MAX_QUEUE_LENGTH));
      unlink_queues_.push_back(unlink_queue);
      local_unlink_queues_.emplace_back();
    }
  }

  virtual ~TransactionLevelGCManager() {}

  // this function cleans up all the member variables in the class object.
  virtual void Reset() override {
    unlink_queues_.clear();
    local_unlink_queues_.clear();

    unlink_queues_.reserve(gc_thread_count_);
    for (int i = 0; i < gc_thread_count_; ++i) {
      std::shared_ptr<LockFreeQueue<concurrency::TransactionContext* >>
          unlink_queue(new LockFreeQueue<concurrency::TransactionContext* >(
              MAX_QUEUE_LENGTH));
      unlink_queues_.push_back(unlink_queue);
      local_unlink_queues_.emplace_back();
    }

    reclaim_maps_.clear();
    reclaim_maps_.resize(gc_thread_count_);
    recycle_queue_map_.clear();

    is_running_ = false;
  }

  static TransactionLevelGCManager &GetInstance(const int thread_count = 1) {
    static TransactionLevelGCManager gc_manager(thread_count);
    return gc_manager;
  }

  virtual void StartGC(
      std::vector<std::unique_ptr<std::thread>> &gc_threads) override {
    LOG_TRACE("Starting GC");
    this->is_running_ = true;
    gc_threads.resize(gc_thread_count_);
    for (int i = 0; i < gc_thread_count_; ++i) {
      gc_threads[i].reset(
          new std::thread(&TransactionLevelGCManager::Running, this, i));
    }
  }

  virtual void StartGC() override {
    LOG_TRACE("Starting GC");
    this->is_running_ = true;
    for (int i = 0; i < gc_thread_count_; ++i) {
      thread_pool.SubmitDedicatedTask(&TransactionLevelGCManager::Running, this,
                                      std::move(i));
    }
  };

  /**
   * @brief This stops the Garbage Collector when Peloton shuts down
   *
   * @return No return value.
   */
  virtual void StopGC() override;

  virtual void RecycleTransaction(
      concurrency::TransactionContext *txn) override;

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id) override;

  virtual void RegisterTable(const oid_t &table_id) override {
    // Insert a new entry for the table
    if (recycle_queue_map_.find(table_id) == recycle_queue_map_.end()) {
      std::shared_ptr<LockFreeQueue<ItemPointer>> recycle_queue(
          new LockFreeQueue<ItemPointer>(MAX_QUEUE_LENGTH));
      recycle_queue_map_[table_id] = recycle_queue;
    }
  }

  virtual void DeregisterTable(const oid_t &table_id) override {
    // Remove dropped tables
    if (recycle_queue_map_.find(table_id) != recycle_queue_map_.end()) {
      recycle_queue_map_.erase(table_id);
    }
  }

  virtual size_t GetTableCount() override { return recycle_queue_map_.size(); }

  int Unlink(const int &thread_id, const eid_t &expired_eid);

  int Reclaim(const int &thread_id, const eid_t &expired_eid);

 private:
  inline unsigned int HashToThread(const size_t &thread_id) {
    return (unsigned int)thread_id % gc_thread_count_;
  }

  /**
   * @brief Unlink and reclaim the tuples remained in a garbage collection
   * thread when the Garbage Collector stops.
   *
   * @return No return value.
   */
  void ClearGarbage(int thread_id);

  void Running(const int &thread_id);

  void AddToRecycleMap(concurrency::TransactionContext *txn_ctx);

  bool ResetTuple(const ItemPointer &);

  // this function iterates the gc context and unlinks every version
  // from the indexes.
  // this function will call the UnlinkVersion() function.
  void UnlinkVersions(concurrency::TransactionContext *txn_ctx);

  // this function unlinks a specified version from the index.
  void UnlinkVersion(const ItemPointer location, const GCVersionType type);

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  int gc_thread_count_;

  // queues for to-be-unlinked tuples.
  // # unlink_queues == # gc_threads
  std::vector<std::shared_ptr<
      peloton::LockFreeQueue<concurrency::TransactionContext* >>>
      unlink_queues_;

  // local queues for to-be-unlinked tuples.
  // # local_unlink_queues == # gc_threads
  std::vector<
      std::list<concurrency::TransactionContext* >> local_unlink_queues_;

  // multimaps for to-be-reclaimed tuples.
  // The key is the timestamp when the garbage is identified, value is the
  // metadata of the garbage.
  // # reclaim_maps == # gc_threads
  std::vector<std::multimap<cid_t, concurrency::TransactionContext* >>
      reclaim_maps_;

  // queues for to-be-reused tuples.
  // # recycle_queue_maps == # tables
  std::unordered_map<oid_t,
                     std::shared_ptr<peloton::LockFreeQueue<ItemPointer>>>
      recycle_queue_map_;
};
}
}  // namespace peloton
