//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// libevent_thread.h
//
// Identification: src/include/wire/libevent_thread.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#include <arpa/inet.h>
#include <netinet/tcp.h>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <vector>

#include <sys/file.h>
#include <fstream>

#include "common/exception.h"
#include "common/logger.h"
#include "configuration/configuration.h"
#include "container/lock_free_queue.h"
#include "wire/libevent_server.h"
#include "wire/packet_manager.h"

namespace peloton {
namespace wire {

// Forward Declarations
struct NewConnQueueItem;

class LibeventThread {
 protected:
  // The connection thread id
  const int thread_id_;
  struct event_base *libevent_base_;
 public:
  bool is_started = false;
  bool is_closed = false;

  LibeventThread(const int thread_id, struct event_base *libevent_base)
      : thread_id_(thread_id), libevent_base_(libevent_base) {
    if (libevent_base_ == nullptr) {
      LOG_ERROR("Can't allocate event base\n");
      exit(1);
    }
  };

  inline struct event_base *GetEventBase() { return libevent_base_; }

  inline int GetThreadID() const { return thread_id_; }

  // TODO implement destructor
  inline ~LibeventThread() {}
};

class LibeventWorkerThread : public LibeventThread {
 public:
  // New connection event
  struct event *new_conn_event_;

  struct event *ev_timeout;
  // Notify new connection pipe(send end)
  int new_conn_send_fd;

  // Notify new connection pipe(receive end)
  int new_conn_receive_fd;

  /* The queue for new connection requests */
  LockFreeQueue<std::shared_ptr<NewConnQueueItem>> new_conn_queue;

 public:
  LibeventWorkerThread(const int thread_id);
};

// a master thread contains multiple worker threads.
class LibeventMasterThread : public LibeventThread {
 private:
  const int num_threads_;

  // TODO: have a smarter dispatch scheduler
  std::atomic<int> next_thread_id_;  // next thread we dispatched to

 public:
  LibeventMasterThread(const int num_threads, struct event_base *libevent_base);

  void DispatchConnection(int new_conn_fd, short event_flags);

  void CloseConnection();

  std::vector<std::shared_ptr<LibeventWorkerThread>> &GetWorkerThreads();

  static void StartWorker(peloton::wire::LibeventWorkerThread *worker_thread);
};

}  // namespace wire
}  // namespace peloton
