//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_thread.h
//
// Identification: src/include/network/network_thread.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>


#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <unordered_set>

#include <sys/file.h>

#include "common/exception.h"
#include "common/logger.h"
#include "container/lock_free_queue.h"
#include "network_state.h"

namespace peloton {
namespace network {

// Forward Declarations
struct NewConnQueueItem {
  int new_conn_fd;
  short event_flags;
  ConnState init_state;

  inline NewConnQueueItem(int new_conn_fd, short event_flags,
                          ConnState init_state)
      : new_conn_fd(new_conn_fd),
        event_flags(event_flags),
        init_state(init_state) {}
};

class NotifiableTask {
public:
  NotifiableTask(const int task_id)
      : task_id_(task_id) {
    base_ = event_base_new();
    // TODO(tianyu) Error handling here can be better perhaps?
    if (base_ == nullptr) {
      LOG_ERROR("Can't allocate event base\n");
      exit(1);
    }
  };

  // TODO(tianyu) Rename this since it is not an actual thread id
  inline int Id() const { return task_id_; }

  virtual ~NotifiableTask() {
      for (struct event *event : events_)
        event_free(event);
      event_base_free(base_);
  }

  // TODO(tianyu) We can probably get rid of these flags
  // Getter and setter for flags
  bool GetThreadIsStarted() { return is_started; }

  void SetThreadIsStarted(bool is_started) { this->is_started = is_started; }

  bool GetThreadIsClosed() { return is_closed; }

  void SetThreadIsClosed(bool is_closed) { this->is_closed = is_closed; }

  /**
   * @brief Register an event with the event base associated with this notifiable task.
   *
   * After registration, the event firing will result in the callback registered executing on
   * the thread this task is running on. Certain events has the same life cycle as the
   * task itself, in which case it is safe to ignore the return value and have these events
   * be freed on destruction of the task. However, if this is not the case, the caller will
   * need to save the return value and manually unregister the event with the task.
   *
   * @see UnregisterEvent()
   *
   * @param fd the file descriptor or signal to be monitored, or -1. (if manual or time-based)
   * @param flags desired events to monitor: bitfield of EV_READ, EV_WRITE,
         EV_SIGNAL, EV_PERSIST, EV_ET.
   * @param callback callback function to be invoked when the event happens
   * @param arg an argument to be passed to the callback function
   * @param timeout the maximum amount of time to wait for an event, defaults to null which will wait forever
   * @return pointer to the allocated event.
   */
  struct event *RegisterEvent(int fd,
                              short flags,
                              event_callback_fn callback,
                              void *arg,
                              const struct timeval *timeout = nullptr) {
    struct event *event = event_new(base_, fd, flags, callback, arg);
    events_.insert(event);
    event_add(event, timeout);
    return event;
  }

  /**
   * @brief Register a signal event. This is a wrapper around RegisterEvent()
   *
   * @see RegisterEvent(), UnregisterEvent()
   *
   * @param signal Signal number to listen on
   * @param callback callback function to be invoked when the event happens
   * @param arg an argument to be passed to the callback function
   * @param timeout the maximum amount of time to wait for an event, defaults to null which will wait forever
   * @return pointer to the allocated event.
   */
  struct event *RegisterSignalEvent(int signal,
                                    event_callback_fn callback,
                                    void *arg,
                                    const struct timeval *timeout = nullptr) {
    return RegisterEvent(signal, EV_SIGNAL|EV_PERSIST, callback, arg, timeout);
  }

  /**
   * @brief Register an event that fires periodically based on the given time interval.
   * This is a wrapper around RegisterEvent()
   *
   * @see RegisterEvent(), UnregisterEvent()
   *
   * @param timeout period of wait between each firing of this event
   * @param callback callback function to be invoked when the event happens
   * @param arg an argument to be passed to the callback function
   * @return pointer to the allocated event.
   */
  struct event *RegisterPeriodicEvent(const struct timeval *timeout,
                                      event_callback_fn callback,
                                      void *arg) {
    return RegisterEvent(-1, EV_TIMEOUT|EV_PERSIST, callback, arg, timeout);
  }


  struct event *RegisterManualEvent(event_callback_fn callback, void *arg) {
    return RegisterEvent(-1, EV_PERSIST, callback, arg);
  }

  /**
   * @brief Unregister the event given. The event is no longer active and its memory is freed
   *
   * The event pointer must be handed out from an earlier call to RegisterEvent.
   *
   * @see RegisterEvent()
   *
   * @param event the event to be freed
   */
  void UnregisterEvent(struct event *event) {
    auto it = events_.find(event);
    if (it == events_.end()) return;
    if (event_del(event) == -1) {
      LOG_ERROR("Failed to delete event");
      return;
    }
    event_free(event);
    events_.erase(event);
  }

  /**
   * In a loop, make this notifiable task wait and respond to incoming events
   */
  void EventLoop() {
    LOG_TRACE("stop");
    event_base_dispatch(base_);
  }

  /**
   * Exits the event loop
   */
  void Break() {
    event_base_loopexit(base_, nullptr);
  }

private:
  // The connection thread id
  const int task_id_;
  struct event_base *base_;
  bool is_started = false;
  bool is_closed = false;
  int sock_fd = -1;

  // For deallocation
  std::unordered_set<struct event *> events_;
};

}  // namespace network
}  // namespace peloton
