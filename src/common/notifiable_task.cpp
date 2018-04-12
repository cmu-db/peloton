//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// notifiable_task.cpp
//
// Identification: src/common/notifiable_task.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/notifiable_task.h"
#include "common/logger.h"
#include "common/event_util.h"
#include <cstring>

namespace peloton {

NotifiableTask::NotifiableTask(int task_id) : task_id_(task_id) {
  base_ = EventUtil::EventBaseNew();
  // For exiting a loop
  terminate_ = RegisterManualEvent([](int, short, void *arg) {
    EventUtil::EventBaseLoopExit((struct event_base *) arg, nullptr);
  }, base_);
};

NotifiableTask::~NotifiableTask() {
  for (struct event *event : events_) {
    EventUtil::EventDel(event);
    event_free(event);
  }
  event_base_free(base_);
}

struct event *NotifiableTask::RegisterEvent(int fd,
                                            short flags,
                                            event_callback_fn callback,
                                            void *arg,
                                            const struct timeval *timeout) {
  struct event *event = event_new(base_, fd, flags, callback, arg);
  events_.insert(event);
  EventUtil::EventAdd(event, timeout);
  return event;
}

void NotifiableTask::UnregisterEvent(struct event *event) {
  auto it = events_.find(event);
  if (it == events_.end()) return;
  if (event_del(event) == -1) {
    LOG_ERROR("Failed to delete event");
    return;
  }
  event_free(event);
  events_.erase(event);
}

}  // namespace peloton

