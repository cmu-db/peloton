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
#include "common/internal_types.h"
#include "common/container/lock_free_queue.h"
#include "concurrency/transaction_context.h"
#include "gc/gc_manager.h"
#include "gc/recycle_stack.h"

namespace peloton {

namespace gc {

static constexpr size_t INITIAL_UNLINK_QUEUE_LENGTH = 100000;
static constexpr size_t INITIAL_TG_QUEUE_LENGTH = 1000;
static constexpr size_t INITIAL_MAP_SIZE = 32;
static constexpr size_t MAX_ATTEMPT_COUNT = 100000;
//static constexpr size_t INITIAL_TABLE_SIZE = 128;

class TransactionLevelGCManager : public GCManager {

 public:
  TransactionLevelGCManager(const int thread_count)
      : gc_thread_count_(thread_count), local_unlink_queues_(thread_count), reclaim_maps_(thread_count) {

    unlink_queues_.reserve(thread_count);
    immutable_tile_group_queues_.reserve(thread_count);

    for (int i = 0; i < gc_thread_count_; ++i) {

      unlink_queues_.emplace_back(std::make_shared<
          LockFreeQueue<concurrency::TransactionContext* >>(INITIAL_UNLINK_QUEUE_LENGTH));

      immutable_tile_group_queues_.emplace_back(std::make_shared<
          LockFreeQueue<oid_t>>(INITIAL_TG_QUEUE_LENGTH));
    }

    recycle_stacks_ = std::make_shared<peloton::CuckooMap<
        oid_t, std::shared_ptr<RecycleStack>>>(INITIAL_MAP_SIZE);
  }

  virtual ~TransactionLevelGCManager() {}

  // this function cleans up only the member variables in the class object.
  // leaks tuples slots, txns, etc. if StopGC() not called first
  // only used for testing purposes currently
  virtual void Reset() override {

    local_unlink_queues_.clear();
    local_unlink_queues_.resize(gc_thread_count_);

    reclaim_maps_.clear();
    reclaim_maps_.resize(gc_thread_count_);

    unlink_queues_.clear();
    unlink_queues_.reserve(gc_thread_count_);

    immutable_tile_group_queues_.clear();
    immutable_tile_group_queues_.reserve(gc_thread_count_);

    for (int i = 0; i < gc_thread_count_; ++i) {

      unlink_queues_.emplace_back(std::make_shared<
          LockFreeQueue<concurrency::TransactionContext* >>(INITIAL_UNLINK_QUEUE_LENGTH));

      immutable_tile_group_queues_.emplace_back(std::make_shared<
          LockFreeQueue<oid_t>>(INITIAL_TG_QUEUE_LENGTH));
    }

    recycle_stacks_ = std::make_shared<peloton::CuckooMap<
        oid_t, std::shared_ptr<RecycleStack>>>(INITIAL_MAP_SIZE);

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

  // Returns an empty, recycled tuple slot that can be used for insertion
  virtual ItemPointer GetRecycledTupleSlot(storage::DataTable *table) override;

  virtual void RecycleTupleSlot(const ItemPointer &location) override;

  virtual void RegisterTable(oid_t table_id) override {

    // if table already registered, ignore
    if (recycle_stacks_->Contains(table_id)) {
      return;
    }
    // Insert a new entry for the table
    auto recycle_stack = std::make_shared<RecycleStack>();
    recycle_stacks_->Insert(table_id, recycle_stack);
  }

  virtual void DeregisterTable(const oid_t &table_id) override {
    recycle_stacks_->Erase(table_id);
  }

  virtual size_t GetTableCount() override { return recycle_stacks_->GetSize(); }

  int Unlink(const int &thread_id, const eid_t &expired_eid);

  int Reclaim(const int &thread_id, const eid_t &expired_eid);

  /**
* @brief Unlink and reclaim the tuples that remain in a garbage collection
* thread when the Garbage Collector stops. Used primarily by tests. Also used internally
*
* @return No return value.
*/
  void ClearGarbage(int thread_id);

 private:

  // convenience function to get table's recycle queue
  std::shared_ptr<RecycleStack>
  GetTableRecycleStack(const oid_t &table_id) const {
    std::shared_ptr<RecycleStack> recycle_stack;
    if (recycle_stacks_->Find(table_id, recycle_stack)) {
      return recycle_stack;
    } else {
      return nullptr;
    }
  }

  inline unsigned int HashToThread(const size_t &thread_id) {
    return (unsigned int)thread_id % gc_thread_count_;
  }

  void Running(const int &thread_id);

  void RecycleTupleSlots(concurrency::TransactionContext *txn_ctx);

  void RemoveGarbageObjects(concurrency::TransactionContext *txn_ctx);

  bool ResetTuple(const ItemPointer &);

  // iterates the gc context and unlinks every version
  // from the indexes.
  // this function will call the UnlinkVersion() function.
  void RemoveVersionsFromIndexes(concurrency::TransactionContext *txn_ctx);

  // this function unlinks a specified version from the index.
  void RemoveVersionFromIndexes(const ItemPointer location, GCVersionType type);

  // iterates through immutable tile group queue and purges all tile groups
  // from the recycles queues
  int ProcessImmutableTileGroupQueue(oid_t thread_id);

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

  // queues of tile groups to be purged from recycle_stacks
  // oid_t here is tile_group_id
  std::vector<std::shared_ptr<
      peloton::LockFreeQueue<oid_t>>> immutable_tile_group_queues_;

  // queues for to-be-reused tuples.
  // map of tables to recycle stacks
  std::shared_ptr<peloton::CuckooMap<oid_t, std::shared_ptr<
      RecycleStack>>> recycle_stacks_;
};
}
}  // namespace peloton
