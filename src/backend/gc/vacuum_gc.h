//
// Created by Zrs_y on 5/10/16.
//

#pragma once

#include <thread>
#include <unordered_map>
#include <map>
#include <vector>

#include "backend/common/types.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "backend/gc/gc_manager.h"
#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace gc {
class Vacuum_GCManager : public GCManager {
public:
  Vacuum_GCManager()
    : is_running_(true),
      unlink_queue_(MAX_QUEUE_LENGTH),
      free_queue_(MAX_QUEUE_LENGTH) {
    StartGC();
  }

  ~Vacuum_GCManager() { StopGC(); }

  static Vacuum_GCManager& GetInstance() {
    static Vacuum_GCManager gcManager;
    return gcManager;
  }

  // Get status of whether GC thread is running or not
  virtual bool GetStatus() { return this->is_running_; }

  virtual void StartGC();

  virtual void StopGC();

  virtual void RecycleOldTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                                const oid_t &tuple_id, const cid_t &tuple_end_cid);

  virtual void RecycleInvalidTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                                   const oid_t &tuple_id);

  virtual ItemPointer ReturnFreeSlot(const oid_t &table_id);

private:
  void ClearGarbage();

  void Running();

  void Reclaim(const cid_t &max_cid);

  void Unlink(const cid_t &max_cid);

  void DeleteTupleFromIndexes(const TupleMetadata &);

  bool ResetTuple(const TupleMetadata &);

private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//
  volatile bool is_running_;
  GCType gc_type_;

  std::unique_ptr<std::thread> gc_thread_;

  LockfreeQueue<TupleMetadata> unlink_queue_;
  LockfreeQueue<TupleMetadata> free_queue_;

  // Map of actual grabage.
  // The key is the timestamp when the garbage is identified, value is the
  // metadata of the garbage.
  // TODO: use shared pointer to reduce memory copy
  std::multimap<cid_t, TupleMetadata> reclaim_map_;

  // TODO: use shared pointer to reduce memory copy
  // table_id -> queue
  cuckoohash_map<oid_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> recycle_queue_map_;
};
}
}

