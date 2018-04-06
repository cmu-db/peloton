//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// notifiable_task.h
//
// Identification: src/include/common/notifiable_task.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_set>
#include <event2/thread.h>
#include "common/event_util.h"

namespace peloton {

/**
 * Convenient MACRO to use a method as a libevent callback function. Example
 * usage:
 *
 * (..., METHOD_AS_CALLBACK(ConnectionDispatcherTask, DispatchConnection), obj)
 *
 * Would call DispatchConnection method on ConnectionDispatcherTask. obj must be
 * an instance
 * of ConnectionDispatcherTask. The method being invoked must have signature
 * void(int, short),
 * where int is the fd and short is the flags supplied by libevent.
 *
 */
#define METHOD_AS_CALLBACK(type, method)         \
  [](int fd, short flags, void *arg) {           \
    static_cast<type *>(arg)->method(fd, flags); \
  }

/**
 * @brief NotifiableTasks can be configured to handle events with callbacks, and
 * executes within an event loop
 *
 * More specifically, NotifiableTasks are backed by libevent, and takes care of
 * memory management with the library.
 */
class NotifiableTask {
 public:
  /**
   * Constructs a new NotifiableTask instance.
   * @param task_id a unique id assigned to this task
   */
  explicit NotifiableTask(int task_id);

  /**
   * Destructs this NotifiableTask. All events currently registered to its base
   * are also deleted and freed.
   */
  virtual ~NotifiableTask();

  /**
   * @return unique id assigned to this task
   */
  inline int Id() const { return task_id_; }


  /**
   * @brief Register an event with the event base associated with this
   * notifiable task.
   *
   * After registration, the event firing will result in the callback registered
   * executing on the thread this task is running on. Certain events has the
   * same life cycle as the task itself, in which case it is safe to ignore the
   * return value and have these events be freed on destruction of the task.
   * However, if this is not the case, the caller will need to save the return
   * value and manually unregister the event with the task.
   * @see UnregisterEvent().
   *
   * @param fd the file descriptor or signal to be monitored, or -1. (if manual
   *         or time-based)
   * @param flags desired events to monitor: bitfield of EV_READ, EV_WRITE,
   *         EV_SIGNAL, EV_PERSIST, EV_ET.
   * @param callback callback function to be invoked when the event happens
   * @param arg an argument to be passed to the callback function
   * @param timeout the maximum amount of time to wait for an event, defaults to
   *        null which will wait forever
   * @return pointer to the allocated event.
   */
  struct event *RegisterEvent(int fd, short flags, event_callback_fn callback,
                              void *arg,
                              const struct timeval *timeout = nullptr);
  /**
   * @brief Register a signal event. This is a wrapper around RegisterEvent()
   *
   * @see RegisterEvent(), UnregisterEvent()
   *
   * @param signal Signal number to listen on
   * @param callback callback function to be invoked when the event happens
   * @param arg an argument to be passed to the callback function
   * @param timeout the maximum amount of time to wait for an event, defaults to
   * null which will wait forever
   * @return pointer to the allocated event.
   */
  inline struct event *RegisterSignalEvent(int signal,
                                           event_callback_fn callback,
                                           void *arg) {
    return RegisterEvent(signal, EV_SIGNAL | EV_PERSIST, callback, arg);
  }

  /**
   * @brief Register an event that fires periodically based on the given time
   * interval.
   * This is a wrapper around RegisterEvent()
   *
   * @see RegisterEvent(), UnregisterEvent()
   *
   * @param timeout period of wait between each firing of this event
   * @param callback callback function to be invoked when the event happens
   * @param arg an argument to be passed to the callback function
   * @return pointer to the allocated event.
   */
  inline struct event *RegisterPeriodicEvent(const struct timeval *timeout,
                                             event_callback_fn callback,
                                             void *arg) {
    return RegisterEvent(-1, EV_TIMEOUT | EV_PERSIST, callback, arg, timeout);
  }

  /**
   * @brief Register an event that can only be fired if someone calls
   * event_active on it manually.
   * This is a wrapper around RegisterEvent()
   *
   * @see RegisterEvent(), UnregisterEvent()
   *
   * @param callback callback function to be invoked when the event happens
   * @param arg an argument to be passed to the callback function
   * @return pointer to the allocated event.
   */
  inline struct event *RegisterManualEvent(event_callback_fn callback,
                                           void *arg) {
    return RegisterEvent(-1, EV_PERSIST, callback, arg);
  }

  // TODO(tianyu): The original network code seems to do this as an
  //  optimization. Specifically it avoids new memory allocation by reusing
  //  an existing event. I am leaving this out until we get numbers.
  //  void UpdateEvent(struct event *event, int fd, short flags,
  //                   event_callback_fn callback, void *arg,
  //                   const struct timeval *timeout = nullptr) {
  //    PELOTON_ASSERT(!(events_.find(event) == events_.end()));
  //    EventUtil::EventDel(event);
  //    EventUtil::EventAssign(event, base_, fd, flags, callback, arg);
  //    EventUtil::EventAdd(event, timeout);
  //  }
  //
  //  void UpdateManualEvent(struct event *event, event_callback_fn callback,
  //                         void *arg) {
  //    UpdateEvent(event, -1, EV_PERSIST, callback, arg);
  //  }

  /**
   * @brief Unregister the event given. The event is no longer active and its
   * memory is freed
   *
   * The event pointer must be handed out from an earlier call to RegisterEvent.
   *
   * @see RegisterEvent()
   *
   * @param event the event to be freed
   */
  void UnregisterEvent(struct event *event);

  /**
   * In a loop, make this notifiable task wait and respond to incoming events
   */
  inline void EventLoop() {
    EventUtil::EventBaseDispatch(base_);
    LOG_TRACE("stop");
  }
  /**
   * Exits the event loop
   */
  virtual void ExitLoop() { event_active(terminate_, 0, 0); }

  /**
   * Wrapper around ExitLoop() to conform to libevent callback signature
   */
  inline void ExitLoop(int, short) { ExitLoop(); }

 private:
  const int task_id_;
  struct event_base *base_;

  // struct event and lifecycle management
  struct event *terminate_;
  std::unordered_set<struct event *> events_;
};

}  // namespace peloton
