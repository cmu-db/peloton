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

#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/event.h>

#include <arpa/inet.h>
#include <netinet/tcp.h>

#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include "common/logger.h"
#include "common/config.h"
#include "wire/wire.h"
#include "wire/socket_base.h"
#include "container/lock_free_queue.h"
#include <sys/file.h>
#include <fstream>

namespace peloton {

namespace wire {

class LibeventThread {

 public:
  LibeventThread(unsigned int thread_id);

  // TODO implement destructor
  inline ~LibeventThread() {}

  // XXX Should we make this non-static?
  static void CreateConnection(int client_fd, struct event_base *base);

  static void ProcessConnection(evutil_socket_t fd, short event, void *arg);

  // XXX Should we make this non-static?
  static void DispatchConnection();

  static void EventHandler(evutil_socket_t fd, short event, void *arg);

  static void Init(int num_threads);

  static void Loop(peloton::wire::LibeventThread *libevent_thread);

  static std::shared_ptr<LibeventThread> GetLibeventThread(
      unsigned int conn_thread_id);

  static unsigned int connection_thread_id;

  static int num_threads;

 public:
  struct event_base *libevent_base;

  // TODO change back to private
 public:
  // The connection thread id
  unsigned int thread_id_;

  // New connection event
  struct event *new_conn_event_;

  // Notify new connection pipe(receive end)
  int new_conn_receive_fd_;

  // Notify new connection pipe(send end)
  int new_conn_send_fd_;

  // The queue for new connection requests
  // LockFreeQueue<std::shared_ptr<int64_t >> new_conn_queue;

 private:
  static std::vector<std::shared_ptr<LibeventThread>> &GetLibeventThreads();
};
}
}
