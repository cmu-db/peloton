//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dedicated_thread_registry.h
//
// Identification: src/include/common/dedicated_thread_registry.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <unordered_map>
#include <vector>
#include <memory>
#include "common/macros.h"
#include "common/dedicated_thread_owner.h"
#include "common/dedicated_thread_task.h"

namespace peloton {
class DedicatedThreadRegistry {
 public:
  DedicatedThreadRegistry() = default;
  ~DedicatedThreadRegistry() {
    for (auto &entry : thread_owners_table_) {
      for (auto &task : entry.second) {
        entry.first->NotifyThreadRemoved(task);
        threads_table[task.get()].join();
      }
    }
  }

  // TODO(tianyu): Remove when we remove singletons
  static DedicatedThreadRegistry &GetInstance() {
    static DedicatedThreadRegistry registry;
    return registry;
  }

  template<typename Task, typename... Args>
  std::shared_ptr<Task> RegisterThread(DedicatedThreadOwner *requester,
                      Args... args) {
    auto task = std::make_shared<Task>(args...);
    thread_owners_table_[requester].push_back(task);
    requester->NotifyNewThread();
    threads_table.emplace(task.get(), std::thread([=]{ task->RunTask(); }));
    return task;
  }

  // TODO(tianyu): Add code for thread removal

 private:
  // Using raw pointer is okay since we never dereference said pointer,
  // but only use it as a lookup key
  std::unordered_map<DedicatedThreadTask *, std::thread> threads_table;
  // Using raw pointer here is also fine since the owner's life cycle is
  // not controlled by the registry
  std::unordered_map<DedicatedThreadOwner *,
                     std::vector<std::shared_ptr<DedicatedThreadTask>>>
      thread_owners_table_;
};
}
