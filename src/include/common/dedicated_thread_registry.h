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
#include <memory>
#include <unordered_map>
#include <vector>
#include "common/dedicated_thread_owner.h"
#include "common/dedicated_thread_task.h"
#include "common/macros.h"

namespace peloton {
/**
 * Singleton class responsible for maintaining and dispensing long running
 * (dedicated) threads to other system components. The class also serves
 * as a control panel for the brain component to be able to collect information
 * on threads in the system and modify how threads are allocated.
 */
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

  /**
   *
   * @tparam Task The type of Task to be run on the new thread. Must be a
   *              subtype of DedicatedThreadTask
   * @tparam Args types of arguments to be supplied to constructor of Task
   * @param requester The owner to assign the new thread to
   * @param args the arguments to pass to constructor of task
   * @return the DedicatedThreadTask running on new thread
   */
  template <typename Task, typename... Args>
  std::shared_ptr<Task> RegisterThread(DedicatedThreadOwner *requester,
                                       Args... args) {
    auto task = std::make_shared<Task>(args...);
    thread_owners_table_[requester].push_back(task);
    requester->NotifyNewThread();
    threads_table.emplace(task.get(), std::thread([=] { task->RunTask(); }));
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
}  // namespace peloton
