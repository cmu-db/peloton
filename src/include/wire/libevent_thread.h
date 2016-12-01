//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wire.h
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
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "container/lock_free_queue.h"
#include "wire/libevent_server.h"
#include "wire/wire.h"

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
 private:
  // New connection event
  struct event *new_conn_event_;

 public:
  // Notify new connection pipe(send end)
  int new_conn_send_fd;

  // Notify new connection pipe(receive end)
  int new_conn_receive_fd;

  /* The queue for new connection requests */
  LockFreeQueue<std::shared_ptr<NewConnQueueItem>> new_conn_queue;

 public:
  LibeventWorkerThread(const int thread_id);
};

class LibeventMasterThread : public LibeventThread {
 private:
  const int num_threads_;

  // TODO: have a smarter dispatch scheduler
  int next_thread_id_ = 0;  // next thread we dispatched to

 public:
  LibeventMasterThread(const int num_threads, struct event_base *libevent_base);

  void DispatchConnection(int new_conn_fd, short event_flags);

  std::vector<std::shared_ptr<LibeventWorkerThread>> &GetWorkerThreads();

  static void StartWorker(peloton::wire::LibeventWorkerThread *worker_thread);
};

}  // namespace wire
}  // namespace peloton
