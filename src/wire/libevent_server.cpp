//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// libevent_server.cpp
//
// Identification: src/wire/libevent_server.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "wire/libevent_server.h"


#include "event2/thread.h"

#include <fcntl.h>
#include <inttypes.h>
#include <sys/socket.h>
#include <fstream>

#include "common/init.h"
#include "common/macros.h"
#include "common/thread_pool.h"

namespace peloton {
namespace wire {

std::unordered_map<int, std::unique_ptr<LibeventSocket>>
    &LibeventServer::GetGlobalSocketList() {
  // mapping from socket id to socket object.
  static std::unordered_map<int, std::unique_ptr<LibeventSocket>> global_socket_list;

  return global_socket_list;
}

LibeventSocket *LibeventServer::GetConn(const int &connfd) {
  auto &global_socket_list = GetGlobalSocketList();
  if (global_socket_list.find(connfd) != global_socket_list.end()) {
    return global_socket_list.at(connfd).get();
  } else {
    return nullptr;
  }
}

void LibeventServer::CreateNewConn(const int &connfd, short ev_flags,
                                   LibeventThread *thread,
                                   ConnState init_state) {
  auto &global_socket_list = GetGlobalSocketList();
  if (global_socket_list.find(connfd) == global_socket_list.end()) {
    LOG_INFO("create new connection: id = %d", connfd);
  }
  global_socket_list[connfd].reset(
      new LibeventSocket(connfd, ev_flags, thread, init_state));
}

/**
 * Stop signal handling
 */
void Signal_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                     UNUSED_ATTRIBUTE short what, void *arg) {
  struct event_base *base = (event_base *)arg;
  LOG_INFO("stop");
  event_base_loopexit(base, NULL);
}

LibeventServer::LibeventServer() {
  evthread_use_pthreads();
  struct event_base *base = event_base_new();
  evthread_make_base_notifiable(base);
  struct event *evstop;

  // Create our event base
  if (!base) {
    throw ConnectionException("Couldn't open event base");
  }

  // Add hang up signal event
  evstop = evsignal_new(base, SIGHUP, Signal_Callback, base);
  evsignal_add(evstop, NULL);

  // a master thread is responsible for coordinating worker threads.
  std::shared_ptr<LibeventThread> master_thread(
      new LibeventMasterThread(CONNECTION_THREAD_COUNT, base));

  port_ = FLAGS_port;
  max_connections_ = FLAGS_max_connections;

  // For logging purposes
  //  event_enable_debug_mode();
  //  event_set_log_callback(LogCallback);

  // Commented because it's not in the libevent version we're using
  // When we upgrade this should be uncommented
  //  event_enable_debug_logging(EVENT_DBG_ALL);

  // Ignore the broken pipe signal
  // We don't want to exit on write when the client disconnects
  signal(SIGPIPE, SIG_IGN);

  if (FLAGS_socket_family == "AF_INET") {
    struct sockaddr_in sin;
    PL_MEMSET(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = INADDR_ANY;
    sin.sin_port = htons(port_);

    int listen_fd;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (listen_fd < 0) {
      throw ConnectionException("Failed to create listen socket");
    }

    int reuse = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    if (bind(listen_fd, (struct sockaddr *)&sin, sizeof(sin)) < 0) {
      throw ConnectionException("Failed to bind socket to port: " + std::to_string(port_));
    }

    int conn_backlog = 12;
    if (listen(listen_fd, conn_backlog) < 0) {
      throw ConnectionException("Failed to listen to socket");
    }

    LibeventServer::CreateNewConn(listen_fd, EV_READ | EV_PERSIST,
                                  master_thread.get(), CONN_LISTENING);

    LOG_INFO("Listening on port %lu", port_);
    event_base_dispatch(base);
    event_free(evstop);
    event_base_free(base);
  }

  // This socket family code is not implemented yet
  else {
    throw ConnectionException("Unsupported socket family");
  }
}
}
}
