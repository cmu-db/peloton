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
#include <vector>

#include <sys/file.h>

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

 private:
  bool is_started = false;
  bool is_closed = false;
  int sock_fd = -1;

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

  // Getter and setter for flags
  bool GetThreadIsStarted() { return is_started; }

  void SetThreadIsStarted(bool is_started) { this->is_started = is_started; }

  bool GetThreadIsClosed() { return is_closed; }

  void SetThreadIsClosed(bool is_closed) { this->is_closed = is_closed; }

  int GetThreadSockFd() { return sock_fd; }

  void SetThreadSockFd(int fd) { this->sock_fd = fd; }
};

class LibeventWorkerThread : public LibeventThread {
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
  LibeventWorkerThread(const int thread_id);

  // Getters and setters
  event *GetNewConnEvent() { return this->new_conn_event_; }

  event *GetTimeoutEvent() { return this->ev_timeout_; }

  int GetNewConnSendFd() { return this->new_conn_send_fd_; }

  int GetNewConnReceiveFd() { return this->new_conn_receive_fd_; }
};

// a master thread contains multiple worker threads.
class LibeventMasterThread : public LibeventThread {
 private:
  const int num_threads_;

  // TODO: have a smarter dispatch scheduler
  std::atomic<int> next_thread_id_;  // next thread we dispatched to

 public:
  LibeventMasterThread(const int num_threads, struct event_base *libevent_base);

  void Start();

  void Stop();

  void DispatchConnection(int new_conn_fd, short event_flags);

  std::vector<std::shared_ptr<LibeventWorkerThread>> &GetWorkerThreads();

  static void StartWorker(peloton::wire::LibeventWorkerThread *worker_thread);
};

}  // namespace wire
}  // namespace peloton
