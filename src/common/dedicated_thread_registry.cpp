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
#include "common/dedicated_thread_task.h"
#include "common/dedicated_thread_owner.h"

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

template<typename Task>
void DedicatedThreadRegistry::RegisterDedicatedThread(DedicatedThreadOwner *requester,
                                                      std::shared_ptr<Task> task) {
  thread_owners_table_[requester].push_back(task);
  requester->NotifyNewThread();
  threads_table_.emplace(task.get(), std::thread([=] { task->RunTask(); }));
}
}
