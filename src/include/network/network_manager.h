//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_manager.h
//
// Identification: src/include/network/network_manager.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
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
#include "container/lock_free_queue.h"
#include "notifiable_task.h"
#include "connection_dispatcher_task.h"
#include "protocol_handler.h"
#include "network_state.h"

#include <openssl/ssl.h>
#include <openssl/err.h>

namespace peloton {
namespace network {


class NetworkManager {
 private:
  // For logging purposes
  // static void LogCallback(int severity, const char *msg);

  uint64_t port_;             // port number
  size_t max_connections_;    // maximum number of connections

  std::string private_key_file_;
  std::string certificate_file_;

  std::shared_ptr<ConnectionDispatcherTask> dispatcher_task;

  // Flags for controlling server start/close status
  bool is_started_ = false;
  bool is_closed_ = false;

 public:
  static int recent_connfd;
  static SSL_CTX *ssl_context;

 public:
  NetworkManager();

  void StartServer();

  void Break();

  void CloseServer();

  void SetPort(int new_port);

  // Getter and setter for flags
  bool GetIsStarted() { return is_started_; }

  void SetIsStarted(bool is_started) { this->is_started_ = is_started; }

  bool GetIsClosed() { return is_closed_; }

  void SetIsClosed(bool is_closed) { this->is_closed_ = is_closed; }
};


}
}
