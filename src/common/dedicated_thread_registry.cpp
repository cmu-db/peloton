//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dedicated_thread_registry.cpp
//
// Identification: src/common/dedicated_thread_registry.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/dedicated_thread_registry.h"


namespace peloton {

DedicatedThreadRegistry::~DedicatedThreadRegistry() {
  // Note that if registry is shutting down, it doesn't matter whether
  // owners are notified as this class should have the same life cycle
  // as the entire peloton process.

  for (auto &entry : thread_owners_table_) {
    for (auto &task : entry.second) {
      task->Terminate();
      threads_table_[task.get()].join();
    }
  }
}

DedicatedThreadRegistry &DedicatedThreadRegistry::GetInstance() {
  static DedicatedThreadRegistry registry;
  return registry;
}
}