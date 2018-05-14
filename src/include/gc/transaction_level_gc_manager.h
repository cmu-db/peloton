//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_level_gc_manager.h
//
// Identification: src/include/gc/transaction_level_gc_manager.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
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
static constexpr size_t MAX_PROCESSED_COUNT = 100000;

class TransactionLevelGCManager : public GCManager {

 public:
  TransactionLevelGCManager(const int thread_count);

  virtual ~TransactionLevelGCManager() {}

  // This function cleans up only the member variables in the class object.
  // It leaks tuples slots, txns, etc. if StopGC() not called first.
  // Only used for testing purposes currently.
  virtual void Reset() override;

  static TransactionLevelGCManager &GetInstance(const int thread_count = 1);

  virtual void StartGC(
      std::vector<std::unique_ptr<std::thread>> &gc_threads) override;

  virtual void StartGC() override;

  // This stops the Garbage Collector when Peloton shuts down
  virtual void StopGC() override;

  virtual void RegisterTable(oid_t table_id) override;

  virtual void DeregisterTable(const oid_t &table_id) override;


  virtual void RecycleTransaction(
      concurrency::TransactionContext *txn) override;

  // Returns an empty, recycled tuple slot that can be used for insertion
  virtual ItemPointer GetRecycledTupleSlot(storage::DataTable *table) override;

  virtual void RecycleTupleSlot(const ItemPointer &location) override;

  virtual size_t GetTableCount() override { return recycle_stacks_->GetSize(); }

  int Unlink(const int &thread_id, const eid_t &expired_eid);

  int Reclaim(const int &thread_id, const eid_t &expired_eid);

  virtual void AddToImmutableQueue(const oid_t &tile_group_id) override;

  void AddToCompactionQueue(const oid_t &tile_group_id);

  // Unlink and reclaim the tuples that remain in a garbage collection
  // thread when the Garbage Collector stops.
  // Used primarily by tests. Also used internally.
  void ClearGarbage(int thread_id);

  // iterates through immutable tile group queue and purges all tile groups
  // from the recycles queues
  int ProcessImmutableQueue();

  int ProcessCompactionQueue();
  
 private:

  // convenience function to get table's recycle queue
  std::shared_ptr<RecycleStack>
  GetTableRecycleStack(const oid_t &table_id) const;

  inline unsigned int HashToThread(const size_t &thread_id);

  void Running(const int &thread_id);

  void RecycleTupleSlots(concurrency::TransactionContext *txn_ctx);

  void RemoveObjectLevelGarbage(concurrency::TransactionContext *txn_ctx);

  bool ResetTuple(const ItemPointer &);

  // iterates the gc context and unlinks every version
  // from the indexes.
  // this function will call the UnlinkVersion() function.
  void RemoveVersionsFromIndexes(concurrency::TransactionContext *txn_ctx);

  // this function unlinks a specified version from the index.
  void RemoveVersionFromIndexes(const ItemPointer location, GCVersionType type);

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
  std::shared_ptr<peloton::LockFreeQueue<oid_t>> immutable_queue_;

  // queues of tile groups to be compacted
  // oid_t here is tile_group_id
  std::shared_ptr<peloton::LockFreeQueue<oid_t>> compaction_queue_;

  // queues for to-be-reused tuples.
  // map of tables to recycle stacks
  std::shared_ptr<peloton::CuckooMap<oid_t, std::shared_ptr<
      RecycleStack>>> recycle_stacks_;
};
}
}  // namespace peloton
