//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_server.h
//
// Identification: src/include/networking/network_server.h
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
#include "networking/network_thread.h"
#include "networking/protocol_handler.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

#define QUEUE_SIZE 100
#define MASTER_THREAD_ID -1

namespace peloton {
namespace networking {

// Forward Declarations
class NetworkThread;

// Network Thread States
enum ConnState {
  CONN_LISTENING,         // State that listens for new connections
  CONN_READ,              // State that reads data from the network
  CONN_WRITE,             // State the writes data to the network
  CONN_WAIT,              // State for waiting for some event to happen
  CONN_PROCESS,           // State that runs the wire protocol on received data
  CONN_CLOSING,           // State for closing the client connection
  CONN_CLOSED,            // State for closed connection
  CONN_INVALID,           // Invalid State
  CONN_PARSE_PACKET,      // State for parsing packet from buffers
};

enum ReadState {
  READ_DATA_RECEIVED,
  READ_NO_DATA_RECEIVED,
  READ_ERROR,
};

enum WriteState {
  WRITE_COMPLETE,   // Write completed
  WRITE_NOT_READY,  // Socket not ready to write
  WRITE_ERROR,      // Some error happened
};

/* Libevent Callbacks */

/* Used by a worker thread to receive a new connection from the main thread and
 * launch the event handler */
void WorkerHandleNewConn(evutil_socket_t local_fd, short ev_flags, void *arg);

/* Used by a worker to execute the main event loop for a connection */
void EventHandler(evutil_socket_t connfd, short ev_flags, void *arg);

/* Helpers */

/* Runs the state machine for the protocol. Invoked by event handler callback */
void StateMachine(NetworkManager *network_manager);

/* Set the socket to non-blocking mode */
inline void SetNonBlocking(evutil_socket_t fd) {
  auto flags = fcntl(fd, F_GETFL);
  flags |= O_NONBLOCK;
  if (fcntl(fd, F_SETFL, flags) < 0) {
    LOG_ERROR("Failed to set non-blocking socket");
  }
}

/* Set TCP No Delay for lower latency */
inline void SetTCPNoDelay(evutil_socket_t fd) {
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}


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


struct NetworkServer {
 private:
  // For logging purposes
  // static void LogCallback(int severity, const char *msg);

  uint64_t port_;             // port number
  size_t max_connections_;    // maximum number of connections

  std::string private_key_file_;
  std::string certificate_file_;

  struct event *ev_stop_;     // libevent stop event
  struct event *ev_timeout_;  // libevent timeout event
  std::shared_ptr<NetworkThread> master_thread_;
  struct event_base *base_;  // libevent event_base

  // Flags for controlling server start/close status
  bool is_started_ = false;
  bool is_closed_ = false;

 public:
  static int recent_connfd;
  static SSL_CTX *ssl_context;

 public:
  NetworkServer();

  static NetworkManager *GetConn(const int &connfd);

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
  static std::unordered_map<int, std::unique_ptr<NetworkManager>> &
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
