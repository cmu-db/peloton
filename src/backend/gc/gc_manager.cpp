/*-------------------------------------------------------------------------
 *
 * logmanager.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/gc/gc_manager.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/common/types.h"
#include "backend/gc/gc_manager.h"

namespace peloton {
namespace gc {

/**
 * @brief Return the singleton gc manager instance
 */
GCManager &GCManager::GetInstance() {
  static GCManager gc_manager;
  return gc_manager;
}

GCManager::GCManager() {
  this->status = GC_STATUS_OFF;
}

GCManager::~GCManager() {}

bool GCManager::GetStatus() {
  return this->status;
}

void GCManager::SetStatus(GCStatus status) {
  this->status = status;
}

void GCManager::Poll() {
  LOG_INFO("Polling GC thread...");
  /*
   * Check if we can move anything from the possibly free list to the free
   * list.
   */

  std::this_thread::sleep_for(std::chrono::seconds(5));
  Poll();
}

void GCManager::AddPossiblyFreeTuple(struct TupleMetadata tm) {
  this->possibly_free_list.push_back(tm);
}

}  // namespace gc
}  // namespace peloton
