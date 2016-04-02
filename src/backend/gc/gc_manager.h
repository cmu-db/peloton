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

 private:
  GCStatus status;
  GCManager();
  ~GCManager();
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

};

}  // namespace gc
}  // namespace peloton
