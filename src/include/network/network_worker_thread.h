//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_worker_thread.h
//
// Identification: src/include/network/network_worker_thread.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#include <unistd.h>

#include "common/exception.h"
#include "common/logger.h"
#include "common/container/lock_free_queue.h"
#include "network_thread.h"
#include "network_callback_util.h"

#define QUEUE_SIZE 100

namespace peloton {
namespace network {

class NetworkWorkerThread : public NetworkThread {
 private:
  // New connection event
  struct event *new_conn_event_;

  // Timeout event
  struct event *ev_timeout_;

  // Notify new connection pipe(send end)
  int new_conn_send_fd_;

  // Notify new connection pipe(receive end)
  int new_conn_receive_fd_;

 public:
  /* The queue for new connection requests */
  LockFreeQueue<std::shared_ptr<NewConnQueueItem>> new_conn_queue;

 public:
  NetworkWorkerThread(const int thread_id);

  // Getters and setters
  event *GetNewConnEvent() { return this->new_conn_event_; }

  event *GetTimeoutEvent() { return this->ev_timeout_; }

  int GetNewConnSendFd() { return this->new_conn_send_fd_; }

  int GetNewConnReceiveFd() { return this->new_conn_receive_fd_; }
};

}  // namespace network
}  // namespace peloton
