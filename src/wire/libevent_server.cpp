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
#include <fstream>

#include "wire/libevent_server.h"
#include "common/macros.h"
#include "common/init.h"
#include "common/config.h"
#include "common/thread_pool.h"

namespace peloton {
namespace wire {

std::vector<std::unique_ptr<LibeventSocket>> &
LibeventServer::GetGlobalSocketList() {
  // 2 fd's per thread for pipe and 1 listening socket
  static std::vector<std::unique_ptr<LibeventSocket>>
      global_socket_list(FLAGS_max_connections +
                         std::thread::hardware_concurrency() * 2 + 1);
  return global_socket_list;
}

LibeventSocket *LibeventServer::GetConn(const int &connfd) {
  auto &global_socket_list = GetGlobalSocketList();
  return global_socket_list[connfd].get();
}

void LibeventServer::CreateNewConn(const int &connfd, short ev_flags,
                                   LibeventThread *thread,
                                   ConnState init_state) {
  auto &global_socket_list = GetGlobalSocketList();
  global_socket_list[connfd]
      .reset(new LibeventSocket(connfd, ev_flags, thread, init_state));
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

/**
 * Process refill the buffer and process all packets that can be processed
 */
void ManageRead(LibeventSocket **conn) {
#ifdef LOG_INFO_ENABLED
  std::ostringstream ss;
  ss << std::this_thread::get_id();
  std::string id_str = ss.str();
#endif
  LOG_INFO("New thread %s started execution for connfd %u", id_str.c_str(),
           (*conn)->sock_fd);
  //  // Startup packet
  //  if (!(*conn)->pkt_manager->ManageStartupPacket()) {
  //	  LOG_INFO(
  //	  "Thread %s Executing for socket manager %u failed to manage packet",
  //	  id_str.c_str(), (*conn)->sock_fd);
  //      close((*conn)->sock_fd);
  //    return;
  //  }
  //
  //  // Regular packet
  //  if (!(*conn)->pkt_manager->ManagePacket() ||
  //      (*conn)->is_disconnected == true) {
  //    LOG_INFO(
  //        "Thread %s Executing for socket manager %u failed to manage packet",
  //        id_str.c_str(), (*conn)->sock_fd);
  //    close((*conn)->sock_fd);
  //    return;
  //  }
}


/**
 * This function will be called by libevent when there is a connection
 * ready to be accepted.
 */
void AcceptCallback(UNUSED_ATTRIBUTE struct evconnlistener *listener,
                    evutil_socket_t client_fd,
                    UNUSED_ATTRIBUTE struct sockaddr *address,
                    UNUSED_ATTRIBUTE int socklen, UNUSED_ATTRIBUTE void *ctx) {

  LOG_INFO("New connection on fd %d", int(client_fd));

  // No delay for TCP
  SetTCPNoDelay(client_fd);

  // Set non blocking
  SetNonBlocking(client_fd);

  /* We've accepted a new client, allocate a socket manager to
     maintain the state of this client. */
}

LibeventServer::LibeventServer() {
  struct event_base *base = event_base_new();
  struct event *evstop;

  // Create our event base
  if (!base) {
    LOG_INFO("Couldn't open event base");
    exit(EXIT_FAILURE);
  }
  // Add hang up signal event
  evstop = evsignal_new(base, SIGHUP, Signal_Callback, base);
  evsignal_add(evstop, NULL);

  // TODO: Make pool size a global
  std::shared_ptr<LibeventThread> master_thread(
      new LibeventMasterThread(std::thread::hardware_concurrency(), base));

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
      LOG_ERROR("Failed to create listen socket");
      exit(EXIT_FAILURE);
    }

    if (bind(listen_fd, (struct sockaddr *)&sin, sizeof(sin)) < 0) {
      LOG_ERROR("Failed to bind");
      exit(EXIT_FAILURE);
    }

    int conn_backlog = 12;
    if (listen(listen_fd, conn_backlog) < 0) {
      LOG_ERROR("Failed to listen to socket");
      exit(EXIT_FAILURE);
    }

    int reuse = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    LibeventServer::CreateNewConn(listen_fd, EV_READ | EV_PERSIST,
                                  master_thread.get(), CONN_LISTENING);

    event_base_dispatch(base);
    event_free(evstop);
    event_base_free(base);
  }

  // This socket family code is not implemented yet
  else {
    LOG_ERROR("Unsupported socket family");
    exit(1);
  }
}
}
}
