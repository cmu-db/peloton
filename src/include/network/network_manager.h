//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_manager.h
//
// Identification: src/include/network/network_manager.h
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
#include "network_thread.h"
#include "protocol_handler.h"
#include "network_connection.h"
#include "network_state.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

#define QUEUE_SIZE 100
#define MASTER_THREAD_ID -1

namespace peloton {
namespace network {

// Forward Declarations
class NetworkThread;
class NetworkMasterThread;

/* Network Callbacks */

/* Used by a worker thread to receive a new connection from the main thread and
 * launch the event handler */
void WorkerHandleNewConn(evutil_socket_t local_fd, short ev_flags, void *arg);


/* Helpers */

/* Runs the state machine for the protocol. Invoked by event handler callback */
void StateMachine(NetworkConnection *conn);



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


struct NetworkManager {
 private:
  // For logging purposes
  // static void LogCallback(int severity, const char *msg);

  uint64_t port_;             // port number
  size_t max_connections_;    // maximum number of connections

  std::string private_key_file_;
  std::string certificate_file_;

  struct event *ev_stop_;     // libevent stop event
  struct event *ev_timeout_;  // libevent timeout event
  std::shared_ptr<NetworkMasterThread> master_thread_;
  struct event_base *base_;  // libevent event_base

  // Flags for controlling server start/close status
  bool is_started_ = false;
  bool is_closed_ = false;

 public:
  static int recent_connfd;
  static SSL_CTX *ssl_context;

 public:
  NetworkManager();

  static NetworkConnection *GetConnection(const int &connfd);

  static void CreateNewConn(const int &connfd, short ev_flags,
                            NetworkThread *thread, ConnState init_state);

  void StartServer();

  void CloseServer();

  void SetPort(int new_port);

  // Getter and setter for flags
  bool GetIsStarted() { return is_started_; }

  void SetIsStarted(bool is_started) { this->is_started_ = is_started; }

  bool GetIsClosed() { return is_closed_; }

  void SetIsClosed(bool is_closed) { this->is_closed_ = is_closed; }

  event_base *GetEventBase() { return base_; }

 private:
  /* Maintain a global list of connections.
   * Helps reuse connection objects when possible
   */
  static std::unordered_map<int, std::unique_ptr<NetworkConnection>> &
  GetGlobalSocketList();
};

/*
 * ControlCallback - Some callback helper functions
 */
class ControlCallback {
 public:
  /* Used to handle signals */
  static void Signal_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                              UNUSED_ATTRIBUTE short what, void *arg);

  /* Used to control server start and close */
  static void ServerControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                                     UNUSED_ATTRIBUTE short what, void *arg);

  /* Used to control thread event loop's begin and exit */
  static void ThreadControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                                     UNUSED_ATTRIBUTE short what, void *arg);
};
}
}
