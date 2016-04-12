/*-------------------------------------------------------------------------
 *
 * gc_manager.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/gc/gc_manager.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <mutex>
#include <thread>
#include <deque>
#include <map>
#include <list>
#include <boost/lockfree/queue.hpp>
#include "backend/common/logger.h"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

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
  static GCManager &GetInstance(void);

  // Get status of whether GC thread is running or not
  bool GetStatus();

  void SetStatus(GCStatus status);
  void Poll();

  void AddPossiblyFreeTuple(struct TupleMetadata tm);
  oid_t ReturnFreeSlot(oid_t db_id, oid_t tb_id);

 private:
  GCStatus status;
  std::map<std::pair<oid_t, oid_t>, boost::lockfree::queue<struct TupleMetadata>*> free_map;
  boost::lockfree::queue<struct TupleMetadata> possibly_free_list;
  std::mutex gc_mutex;
  void DeleteTupleFromIndexes(struct TupleMetadata tm);
  GCManager();
  ~GCManager();
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

};

}  // namespace gc
}  // namespace peloton
