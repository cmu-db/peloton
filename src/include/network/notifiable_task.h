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

  // TODO(tianyu) Encapsulate this
  inline struct event_base *GetEventBase() { return base_; }

  // TODO(tianyu) Rename this since it is not an actual thread id
  inline int GetThreadID() const { return task_id_; }

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

  int GetThreadSockFd() { return sock_fd; }

  void SetThreadSockFd(int fd) { this->sock_fd = fd; }

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
  struct event *RegisterEvent(evutil_socket_t fd,
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
    event_free(event);
    events_.erase(event);
  }

  /**
   * @brief Constant method for getting the time interval of one second to be used in register event
   * @return a pointer to a time interval of one second
   */
  static const struct timeval *OneSecond() const { return &one_second; }

private:
  // The connection thread id
  const int task_id_;
  struct event_base *base_;
  bool is_started = false;
  bool is_closed = false;
  int sock_fd = -1;

  // For deallocation
  std::unordered_set<struct event *> events_;
  static const struct timeval one_second = {1, 0};
};

}  // namespace network
}  // namespace peloton
