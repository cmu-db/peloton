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

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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


class PelotonServer {
public:

  PelotonServer();

  PelotonServer &SetupServer();

  void ServerLoop();

  void Close();

  void SetPort(int new_port);

  static int recent_connfd;
  static SSL_CTX *ssl_context;

private:
  // For logging purposes
  // static void LogCallback(int severity, const char *msg);

  uint64_t port_;             // port number
  int listen_fd_ = -1;         // server socket fd that PelotonServer is listening on
  size_t max_connections_;    // maximum number of connections

  std::string private_key_file_;
  std::string certificate_file_;

  std::shared_ptr<ConnectionDispatcherTask> dispatcher_task;

  // For testing purposes
  bool started;
};

}
}
