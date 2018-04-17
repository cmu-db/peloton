//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// event_util.h
//
// Identification: src/include/common/event_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#include "common/exception.h"
#include "common/logger.h"

namespace peloton {

/**
 * Static utility class with wrappers for libevent functions.
 *
 * Wrapper functions are functions with the same signature and return
 * value as the c-style functions, but consist of an extra return value
 * checking. An exception is thrown instead if something is wrong. Wrappers
 * are great tools for using legacy code in a modern code base.
 */
class EventUtil {
 private:
  template <typename T>
  static inline bool NotNull(T *ptr) {
    return ptr != nullptr;
  }

  static inline bool IsZero(int arg) { return arg == 0; }

  static inline bool NonNegative(int arg) { return arg >= 0; }

  template <typename T>
  static inline T Wrap(T value, bool (*check)(T), const char *error_msg) {
    if (!check(value)) throw NetworkProcessException(error_msg);
    return value;
  }

 public:
  EventUtil() = delete;

  static inline struct event_base *EventBaseNew() {
    return Wrap(event_base_new(), NotNull<struct event_base>,
                "Can't allocate event base");
  }

  static inline int EventBaseLoopExit(struct event_base *base,
                                      const struct timeval *timeout) {
    return Wrap(event_base_loopexit(base, timeout), IsZero,
                "Error when exiting loop");
  }

  static inline int EventDel(struct event *event) {
    return Wrap(event_del(event), IsZero, "Error when deleting event");
  }

  static inline int EventAdd(struct event *event,
                             const struct timeval *timeout) {
    return Wrap(event_add(event, timeout), IsZero, "Error when adding event");
  }

  static inline int EventAssign(struct event *event, struct event_base *base,
                                int fd, short flags, event_callback_fn callback,
                                void *arg) {
    return Wrap(event_assign(event, base, fd, flags, callback, arg), IsZero,
                "Error when assigning event");
  }

  static inline int EventBaseDispatch(struct event_base *base) {
    return Wrap(event_base_dispatch(base), NonNegative,
                "Error in event base dispatch");
  }
};
}  // namespace peloton
