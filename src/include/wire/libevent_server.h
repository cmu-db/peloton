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
#include "container/lock_free_queue.h"
#include "wire/libevent_socket.h"
#include <sys/file.h>
#include <fstream>

#define QUEUE_SIZE 100

// TODO: Rename to libevent_server.h
namespace peloton {

namespace wire {

struct LibeventServer {
 private:
  // For logging purposes
  static void LogCallback(int severity, const char* msg);
  uint64_t port_;             // port number
  size_t max_connections_;  // maximum number of connections

 public:
  LibeventServer();
  static inline LibeventSocket* GetConn(const int& connfd);
  static inline void CreateNewConn(const int& connfd, short ev_flags,
                                   std::shared_ptr<LibeventThread>& thread);

 private:
  /* Maintain a global list of connections.
   * Helps reuse connection objects when possible
   */
  static inline
    std::vector<std::unique_ptr<LibeventSocket>>& GetGlobalSocketList();

};

struct NewConnQueueItem {
  int new_conn_fd;
  // enum conn_states init_state;
  short event_flags;
};


class LibeventThread {

  // TODO change back to private
 public:
  /* libevent handle this thread uses */
  struct event_base *libevent_base_;

  /* listen event for notify pipe */
  struct event *notify_new_conn_;

  /* receiving end of notify pipe */
  int notify_receive_fd_;

  /* sending end of notify pipe */
  int notify_send_fd_;

  /* The queue for new connection requests */
  LockFreeQueue<std::shared_ptr<NewConnQueueItem>> new_conn_queue;


public:
  inline ~LibeventThread() {}

  static void ProcessConnRequest(int num);

  static void MainLoop(void *arg);

  LibeventThread() : new_conn_queue(QUEUE_SIZE) {}
};

  /* Libevent Callbacks */

  /* Used by a worker thread to receive a new connection from the main thread and
   * launch the event handler */
  void WorkerHandleNewConn(evutil_socket_t local_fd, short ev_flags, void *arg);

  /* Used by a worker to execute the main event loop for a connection */
  void EventHandler(evutil_socket_t connfd, short ev_flags, void *arg);

}
}
