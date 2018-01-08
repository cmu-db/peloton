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


#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

#include <sys/file.h>

#include "common/exception.h"
#include "common/logger.h"
#include "common/container/lock_free_queue.h"
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

class NetworkThread {
 protected:
  // The connection thread id
  const int thread_id_;
  struct event_base *libevent_base_;

 private:
  bool is_started = false;
  bool is_closed = false;
  int sock_fd = -1;

 public:
  NetworkThread(const int thread_id, struct event_base *libevent_base)
      : thread_id_(thread_id), libevent_base_(libevent_base) {
    if (libevent_base_ == nullptr) {
      LOG_ERROR("Can't allocate event base\n");
      exit(1);
    }
  };

  inline struct event_base *GetEventBase() { return libevent_base_; }

  inline int GetThreadID() const { return thread_id_; }

  // TODO implement destructor
  inline ~NetworkThread() {}

  // Getter and setter for flags
  bool GetThreadIsStarted() { return is_started; }

  void SetThreadIsStarted(bool is_started) { this->is_started = is_started; }

  bool GetThreadIsClosed() { return is_closed; }

  void SetThreadIsClosed(bool is_closed) { this->is_closed = is_closed; }

  int GetThreadSockFd() { return sock_fd; }

  void SetThreadSockFd(int fd) { this->sock_fd = fd; }
};

}  // namespace network
}  // namespace peloton
