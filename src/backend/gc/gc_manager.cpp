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

GCManager::GCManager() {}

GCManager::~GCManager() {}

bool GCManager::GetStatus() {
  return true;
}

void GCManager::Poll() {
  LOG_DEBUG("Polling GC thread...");
  std::this_thread::sleep_for(std::chrono::seconds(5));
  Poll();
}

}  // namespace gc
}  // namespace peloton
