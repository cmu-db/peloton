//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_server.h
//
// Identification: src/include/network/peloton_server.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#include <arpa/inet.h>
#include <netinet/tcp.h>

#include <pthread.h>
#include <sys/file.h>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "common/container/lock_free_queue.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/dedicated_thread_owner.h"
#include "connection_dispatcher_task.h"
#include "network_state.h"
#include "common/notifiable_task.h"
#include "protocol_handler.h"

#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>

namespace peloton {
namespace network {

/**
 * PelotonServer is the entry point of the network layer
 */
class PelotonServer : public DedicatedThreadOwner {
 public:
  /**
   * @brief Constructs a new PelotonServer instance.
   *
   * Note that SettingsManager must already be initialized when this constructor
   * is called.
   */
  PelotonServer();

  /**
   * @brief Configure the server to spin up all its threads and start listening
   * on the configured port.
   *
   * This is separated from the main loop primarily for testing purposes, as we
   * need to wait for the server
   * to start listening on the port before the rest of the test. All
   * event-related settings are also performed
   * here. Since libevent reacts to events fired before the event loop as well,
   * all interactions to the server
   * after this function returns is guaranteed to be handled. For non-testing
   * purposes, you can chain the functions,
   * e.g.:
   *
   *   server.SetupServer().ServerLoop();
   *
   * @return self-reference for chaining
   */
  PelotonServer &SetupServer();

  /**
   * @brief In a loop, handles incoming connection and block the current thread
   * until closed.
   *
   * The loop will exit when either Close() is explicitly called or when there
   * are no more events pending or
   * active (we currently register all events to be persistent.)
   */
  void ServerLoop();

  /**
   * Break from the server loop and exit all network handling threads.
   */
  void Close();

  // TODO(tianyu): This is VILE. Fix this when we refactor testing.
  void SetPort(int new_port);

  static void LoadSSLFileSettings();

  static void SSLInit();

  static int VerifyCallback(int ok, X509_STORE_CTX *store);

  static void SetSSLLevel(SSLLevel ssl_level) { ssl_level_ = ssl_level; }

  static SSLLevel GetSSLLevel() { return ssl_level_; }

  static void SSLLockingFunction(int mode, int n, const char *file, int line);

  static unsigned long SSLIdFunction(void);

  static int SSLMutexSetup(void);

  static int SSLMutexCleanup(void);

  static int recent_connfd;
  static SSL_CTX *ssl_context;
  static std::string private_key_file_;
  static std::string certificate_file_;
  static std::string root_cert_file_;
  static SSLLevel ssl_level_;
  static pthread_mutex_t *ssl_mutex_buf_;

 private:
  // For logging purposes
  // static void LogCallback(int severity, const char *msg);

  uint64_t port_;       // port number
  int listen_fd_ = -1;  // server socket fd that PelotonServer is listening on
  size_t max_connections_;  // maximum number of connections

  std::shared_ptr<ConnectionDispatcherTask> dispatcher_task;

  template <typename... Ts>
  void TrySslOperation(int (*func)(Ts...), Ts... arg);

  // For testing purposes
  std::shared_ptr<ConnectionDispatcherTask> dispatcher_task_;
};
}  // namespace network
}  // namespace peloton
