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

#include "common/container/cuckoo_map.h"
#include "common/container/lock_free_queue.h"
#include "common/init.h"
#include "common/logger.h"
#include "common/thread_pool.h"
#include "common/internal_types.h"
#include "concurrency/transaction_context.h"
#include "gc/gc_manager.h"

namespace peloton {

namespace gc {

using RecycleQueue = peloton::LockFreeQueue<ItemPointer>;

static constexpr size_t QUEUE_LENGTH = 100000;
static constexpr size_t INITIAL_MAP_SIZE = 256;
static constexpr size_t MAX_PROCESSED_COUNT = 100000;

class TransactionLevelGCManager : public GCManager {
 public:
  /**
   * @brief TransactionLevelGCManager should be created with GetInstance()
   */
  TransactionLevelGCManager() = delete;

  /**
   * @brief Resets member variables and data structures to defaults.
   *
   * Intended for testing purposes only.
   *
   * @warning This leaks tuple slots, txns, etc. if StopGC() not called first!
   */
  virtual void Reset() override;

  /**
   *
   * @param[in] thread_count Number of Garbage Collector threads
   * @return Singleton instance of the TransactionLevelGCManager
   */
  static TransactionLevelGCManager &GetInstance(
      const uint32_t &thread_count = 1);

  virtual void StartGC(
      std::vector<std::unique_ptr<std::thread>> &gc_threads) override;

  /**
   * @brief Launches GC threads
   */
  virtual void StartGC() override;

  /**
   * @brief Clears garbage for each GC thread and then ends the threads
   */
  virtual void StopGC() override;

  /**
   * @brief Registers the provided table with the GC to recycle its
   * tuple slots
   * @param[in] table_id Global oid of the table to start recycling
   * slots for
   */
  virtual void RegisterTable(const oid_t &table_id) override;

  /**
   * @brief Deregisters the provided table with the GC to recycle its
   * tuple slots
   * @param[in] table_id Global oid of the table to stop recycling
   * slots for
   */
  virtual void DeregisterTable(const oid_t &table_id) override;

  /**
   * @brief Passes a transaction's context to the GC for freeing and
   * possible recycling
   * @param[id] txn TransactionContext pointer for the GC to process.
   * @warning txn will be freed by the GC, so do not dereference it
   * after calling this function with txn
   */
  virtual void RecycleTransaction(
      concurrency::TransactionContext *txn) override;

  /**
   * @brief Attempt to get a recycled ItemPointer for this table from the GC
   * @param[in] table Pointer of the table to return a recycled ItemPointer for
   * @return ItemPointer to a recycled tuple slot on success,
   * INVALID_ITEMPOINTER otherwise
   */
  virtual ItemPointer GetRecycledTupleSlot(const oid_t &table_id) override;

  /**
   * @brief Recycle the provided tuple slot
   * @param[id] location ItemPointer of the tuple slot to be recycled
   */
  virtual void RecycleTupleSlot(const ItemPointer &location) override;

  /**
   *
   * @return Number of tables currently registered with the GC for recycling
   */
  virtual size_t GetTableCount() override { return recycle_queues_->GetSize(); }

  /**
   * @brief Process unlink queue for provided thread
   * @param[id] thread_id Zero-indexed thread id to access unlink queue
   * @param[id] expired_eid Expired epoch from the EpochManager
   * @return Number of processed tuples
   */
  uint32_t Unlink(const uint32_t &thread_id, const eid_t &expired_eid);

  /**
   * @brief Process reclaim queue for provided thread
   * @param[id] thread_id Zero-indexed thread id to access reclaim queue
   * @param[id] expired_eid Expired epoch from the EpochManager
   * @return Number of processed objects
   */
  uint32_t Reclaim(const uint32_t &thread_id, const eid_t &expired_eid);

  /**
   * @brief Unlink and reclaim the objects currently in queues
   *
   * Meant to be used primarily internally by GC and in tests, not
   * by outside classes
   *
   * @param[in] thread_id
   */
  void ClearGarbage(const uint32_t &thread_id);

 private:
  TransactionLevelGCManager(const uint32_t &thread_count);

  virtual ~TransactionLevelGCManager() {}

  inline unsigned int HashToThread(const size_t &thread_id);

  /**
   * @brief Primary function for GC threads: wakes up, runs GC, has
   * exponential backoff if queues are empty
   * @param[id] thread_id Zero-indexed thread id for queue access
   */
  void Running(const uint32_t &thread_id);

  /**
   * @brief Recycles all of the tuple slots in transaction context's GCSet
   * @param[in] txn_ctx TransactionConext pointer containing GCSet to be
   * processed
   */
  void RecycleTupleSlots(concurrency::TransactionContext *txn_ctx);

  /**
   * @brief Recycles all of the objects in transaction context's GCObjectSet
   * @param[in] txn_ctx TransactionConext pointer containing GCObjectSet
   * to be processed
   */
  void RemoveObjectLevelGarbage(concurrency::TransactionContext *txn_ctx);

  /**
   * @brief Resets a tuple slot's version chain info and varlen pool
   * @return True on success, false if TileGroup no longer exists
   */
  bool ResetTuple(const ItemPointer &);

  /**
 * @brief Helper function to easily look up a table's RecycleStack
 * @param[id] table_id Global oid of the table
 * @return Smart pointer to the RecycleStack for the provided table.
 * May be nullptr if the table is not registered with the GC
 */
  std::shared_ptr<RecycleQueue> GetTableRecycleQueue(
      const oid_t &table_id) const;

  /**
   * @brief Unlinks all tuples in GCSet from indexes.
   * @param[in] txn_ctx TransactionConext pointer containing GCSet to be
   * processed
   */
  void RemoveVersionsFromIndexes(concurrency::TransactionContext *txn_ctx);

  // this function unlinks a specified version from the index.
  /**
   * @brief Unlinks provided tuple from indexes
   * @param[in] location ItemPointer to garbage tuple to be processed
   * @param[in] type GCVersionType for the provided garbage tuple
   */
  void RemoveVersionFromIndexes(const ItemPointer &location,
                                const GCVersionType &type);

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  uint32_t gc_thread_count_;

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
  // oid_t here is global DataTable oid
  std::shared_ptr<peloton::CuckooMap<oid_t, std::shared_ptr<RecycleQueue>>>
      recycle_queues_;
};
}
}  // namespace peloton
