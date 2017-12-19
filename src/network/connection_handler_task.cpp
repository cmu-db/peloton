//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_worker_thread.cpp
//
// Identification: src/network/network_worker_thread.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/connection_handler_task.h"

namespace peloton {
namespace network {

/*
* The worker thread creates a pipe for master-worker communication on
* constructor.
*/
ConnectionHandlerTask::ConnectionHandlerTask(const int thread_id)
    : NotifiableTask(thread_id, event_base_new()), new_conn_queue(QUEUE_SIZE) {
  int fds[2];
  if (pipe(fds)) {
    LOG_ERROR("Can't create notify pipe to accept connections");
    exit(1);
  }
  // send_fd is used by the master thread, received_fd used by worker thread
  new_conn_receive_fd_ = fds[0];
  new_conn_send_fd_ = fds[1];

  // Listen for notifications from the master thread
  new_conn_event_ = event_new(libevent_base_, GetNewConnReceiveFd(),
                              EV_READ | EV_PERSIST,
                              CallbackUtil::WorkerHandleNewConn, this);

  // Check thread's start/close flag every one second
  struct timeval one_seconds = {1, 0};

  ev_timeout_ = event_new(libevent_base_, -1, EV_TIMEOUT | EV_PERSIST,
                          CallbackUtil::ThreadControl_Callback, this);
  event_add(GetTimeoutEvent(), &one_seconds);

  if (event_add(GetNewConnEvent(), 0) == -1) {
    LOG_ERROR("Can't monitor libevent notify pipe\n");
    exit(1);
  }
}

}  // namespace network
}  // namespace peloton