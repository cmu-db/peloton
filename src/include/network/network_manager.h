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
#include <pthread.h>
#include <sys/file.h>

#include "common/exception.h"
#include "common/logger.h"
#include "common/container/lock_free_queue.h"
#include "network_thread.h"
#include "network_master_thread.h"
#include "protocol_handler.h"
#include "network_connection.h"
#include "network_state.h"

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>
#include <openssl/crypto.h>

namespace peloton {
namespace network {

// Forward Declarations
class NetworkThread;
class NetworkMasterThread;
class NetworkConnection;

class NetworkManager {
 private:
  // For logging purposes
  // static void LogCallback(int severity, const char *msg);

  uint64_t port_;             // port number
  size_t max_connections_;    // maximum number of connections


  static SSLLevel ssl_level_;
  static pthread_mutex_t *ssl_mutex_buf_;

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
  static std::string private_key_file_;
  static std::string certificate_file_;
  static std::string root_cert_file_;

 public:
  NetworkManager();

  static NetworkConnection *GetConnection(const int &connfd);

  static void CreateNewConnection(const int &connfd, short ev_flags,
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

  static int verify_callback(int ok, X509_STORE_CTX *store);

  static void SSLInit();

  static void SetSSLLevel(SSLLevel ssl_level) { ssl_level_ = ssl_level; }

  static SSLLevel GetSSLLevel() { return ssl_level_; }

  static void LoadSSLFileSettings();

  static void SSLLockingFunction(int mode, int n, const char* file, int line);

  static unsigned long SSLIdFunction(void);

  static int SSLMutexSetup(void);

  static int SSLMutexCleanup(void);

 private:

  template<typename... Ts> void try_do(int(*func)(Ts...), Ts... arg);

  /* Maintain a global list of connections.
   * Helps reuse connection objects when possible
   */
  static std::unordered_map<int, std::unique_ptr<NetworkConnection>> &
  GetGlobalSocketList();
};


}
}
