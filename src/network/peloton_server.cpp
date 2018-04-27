//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_server.cpp
//
// Identification: src/network/peloton_server.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <fstream>
#include <memory>
#include "common/utility.h"
#include "event2/thread.h"

#include "common/dedicated_thread_registry.h"
#include "network/peloton_rpc_handler_task.h"
#include "network/peloton_server.h"
#include "settings/settings_manager.h"

#include "peloton_config.h"

#define MUTEX_TYPE pthread_mutex_t
#define MUTEX_SETUP(x) pthread_mutex_init(&(x), NULL)
#define MUTEX_CLEANUP(x) pthread_mutex_destroy(&(x))
#define MUTEX_LOCK(x) pthread_mutex_lock(&(x))
#define MUTEX_UNLOCK(x) pthread_mutex_unlock(&(x))
#define THREAD_ID pthread_self()

namespace peloton {
namespace network {

int PelotonServer::recent_connfd = -1;
SSL_CTX *PelotonServer::ssl_context = nullptr;
std::string PelotonServer::private_key_file_;
std::string PelotonServer::certificate_file_;
std::string PelotonServer::root_cert_file_;
SSLLevel PelotonServer::ssl_level_;
MUTEX_TYPE *PelotonServer::ssl_mutex_buf_;

int PelotonServer::SSLMutexSetup(void) {
  int i;
  ssl_mutex_buf_ = new MUTEX_TYPE[CRYPTO_num_locks()];
  if (!ssl_mutex_buf_) return 0;
  for (i = 0; i < CRYPTO_num_locks(); i++) MUTEX_SETUP(ssl_mutex_buf_[i]);
  // register the callback to record the currently-executing thread's identifier
  CRYPTO_set_id_callback(SSLIdFunction);
  // register the callback to perform locking/unlocking
  CRYPTO_set_locking_callback(SSLLockingFunction);
  return 1;
}

int PelotonServer::SSLMutexCleanup(void) {
  int i;
  if (!ssl_mutex_buf_) {
    return 0;
  }
  CRYPTO_set_id_callback(nullptr);
  CRYPTO_set_locking_callback(nullptr);
  // crypto_num_locks(): number of mutex lock
  for (i = 0; i < CRYPTO_num_locks(); i++) {
    MUTEX_CLEANUP(ssl_mutex_buf_[i]);
  }
  delete ssl_mutex_buf_;
  ssl_mutex_buf_ = nullptr;
  return 1;
}

void PelotonServer::SSLLockingFunction(int mode, int n,
                                       UNUSED_ATTRIBUTE const char *file,
                                       UNUSED_ATTRIBUTE int line) {
  if (mode & CRYPTO_LOCK) {
    MUTEX_LOCK(ssl_mutex_buf_[n]);
  } else {
    MUTEX_UNLOCK(ssl_mutex_buf_[n]);
  }
}

unsigned long PelotonServer::SSLIdFunction(void) {
  return ((unsigned long) THREAD_ID);
}

void PelotonServer::LoadSSLFileSettings() {
  private_key_file_ = DATA_DIR + settings::SettingsManager::GetString(
      settings::SettingId::private_key_file);
  certificate_file_ = DATA_DIR + settings::SettingsManager::GetString(
      settings::SettingId::certificate_file);
  root_cert_file_ = DATA_DIR + settings::SettingsManager::GetString(
      settings::SettingId::root_cert_file);
}

void PelotonServer::SSLInit() {
  if (!settings::SettingsManager::GetBool(settings::SettingId::ssl)) {
    SetSSLLevel(SSLLevel::SSL_DISABLE);
    return;
  }

  SetSSLLevel(SSLLevel::SSL_VERIIFY);

  // load error strings for libssl calls(about SSL/TLS protocol)
  SSL_load_error_strings();
  // load error strings for libcrypto calls(about cryptographic algorithms)
  ERR_load_crypto_strings();
  SSL_library_init();
  // For OpenSSL<1.1.0, set up thread callbacks in multithreaded environment.
  // (Not needed if >= 1.1.0) Some global data structures are implicitly shared
  // across threads (error queue...) OpenSSL uses locks to make it thread safe.
  // TODO(Yuchen): deal with returned error 0?
  SSLMutexSetup();
  // set general-purpose version, actual protocol will be negotiated to the
  // highest version  mutually support between client and server during handshake
  ssl_context = SSL_CTX_new(SSLv23_method());
  if (ssl_context == nullptr) {
    SetSSLLevel(SSLLevel::SSL_DISABLE);
    return;
  }
  // TODO(Yuchen): change this.
  // load trusted CA certificates (peer authentication)
  if (SSL_CTX_load_verify_locations(ssl_context, root_cert_file_.c_str(),
                                    nullptr) != 1) {
    LOG_WARN("Exception when loading root_crt!");
    SetSSLLevel(SSLLevel::SSL_PREFER);
  }
  // load OpenSSL's default CA certificate location
  if (SSL_CTX_set_default_verify_paths(ssl_context) != 1) {
    LOG_ERROR("Exception when setting default verify path!");
    SetSSLLevel(SSLLevel::SSL_PREFER);
  }

  LOG_INFO("certificate file path %s", certificate_file_.c_str());
  if (SSL_CTX_use_certificate_chain_file(ssl_context,
                                         certificate_file_.c_str()) != 1) {
    SSL_CTX_free(ssl_context);
    LOG_WARN("Exception when loading server certificate!");
    ssl_context = nullptr;
    SetSSLLevel(SSLLevel::SSL_DISABLE);
    return;
  }

  LOG_INFO("private key file path %s", private_key_file_.c_str());
  if (SSL_CTX_use_PrivateKey_file(ssl_context, private_key_file_.c_str(),
                                  SSL_FILETYPE_PEM) != 1) {
    SSL_CTX_free(ssl_context);
    LOG_ERROR("Exception when loading server key!");
    ssl_context = nullptr;
    SetSSLLevel(SSLLevel::SSL_DISABLE);
    return;
  }

  if (SSL_CTX_check_private_key(ssl_context) != 1) {
    SSL_CTX_free(ssl_context);
    ssl_context = nullptr;
    SetSSLLevel(SSLLevel::SSL_DISABLE);
    return;
  }

  if (GetSSLLevel() == SSLLevel::SSL_VERIIFY) {
    // use built-in function to verify the peer's certificate chain
    // automatically.  set routine to filter the return status of the default
    // verification and returns new verification status.  SSL_VERIFY_PEER: send
    // certificate request to client. Client may ignore the request. If the
    // client sends  back the certificate, it will be verified. Handshake will be
    // terminated if the verification fails.  SSL_VERIFY_FAIL_IF_NO_PEER_CERT: use
    // with SSL_VERIFY_PEER, if client does not send back the certificate,
    // terminate the handshake.
    SSL_CTX_set_verify(ssl_context, SSL_VERIFY_PEER, VerifyCallback);
    SSL_CTX_set_verify_depth(ssl_context, 4);
  } else {
    // SSL_VERIFY_NONE: Server does not request certificate from client.
    SSL_CTX_set_verify(ssl_context, SSL_VERIFY_NONE, VerifyCallback);
  }
  // SSLv2 and SSLv3 are deprecated, shoud not use them
  // TODO(Yuchen): postgres uses SSLv2 | SSLv3 | SSL_OP_SINGLE_DH_USE
  SSL_CTX_set_options(ssl_context, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
  // TODO(Yuchen): adding certificate revocation lists
  // TODO(Yuchen): enable SSL session reuse
  //  int status = SSL_CTX_set_session_id_context(ssl_context,
  //               static_cast<unsigned
  //               char*>(static_cast<void*>(&ssl_context)),
  //               sizeof(ssl_context));
  //  if (!status) {
  //    LOG_ERROR("ssl initialization problem");
  //  }
  // disallow SSL session caching
  SSL_CTX_set_session_cache_mode(ssl_context, SSL_SESS_CACHE_OFF);
}

PelotonServer::PelotonServer() {
  port_ = settings::SettingsManager::GetInt(settings::SettingId::port);
  max_connections_ =
      settings::SettingsManager::GetInt(settings::SettingId::max_connections);

  // For logging purposes
  //  event_enable_debug_mode();

  //  event_set_log_callback(LogCallback);

  // Commented because it's not in the libevent version we're using
  // When we upgrade this should be uncommented
  //  event_enable_debug_logging(EVENT_DBG_ALL);

  // Ignore the broken pipe signal
  // We don't want to exit on write when the client disconnects
  signal(SIGPIPE, SIG_IGN);
}

int PelotonServer::VerifyCallback(int ok, X509_STORE_CTX *store) {
  char data[256];
  if (!ok) {
    X509 *cert = X509_STORE_CTX_get_current_cert(store);
    int depth = X509_STORE_CTX_get_error_depth(store);
    int err = X509_STORE_CTX_get_error(store);
    LOG_ERROR("-Error with certificate at depth: %i", depth);
    X509_NAME_oneline(X509_get_issuer_name(cert), data, 256);
    LOG_ERROR(" issuer = %s", data);
    X509_NAME_oneline(X509_get_subject_name(cert), data, 256);
    LOG_ERROR(" subject = %s", data);
    LOG_ERROR(" err %i:%s", err, X509_verify_cert_error_string(err));
  }
  return ok;
}

template<typename... Ts>
void PelotonServer::TrySslOperation(int (*func)(Ts...), Ts... arg) {
  if (func(arg...) < 0) {
    auto error_message = peloton_error_message();
    if (GetSSLLevel() != SSLLevel::SSL_DISABLE) {
      SSL_CTX_free(ssl_context);
    }
    throw ConnectionException(error_message);
  }
}

PelotonServer &PelotonServer::SetupServer() {
  // This line is critical to performance for some reason
  evthread_use_pthreads();
  if (settings::SettingsManager::GetString(
      settings::SettingId::socket_family) != "AF_INET")
    throw ConnectionException("Unsupported socket family");

  struct sockaddr_in sin;
  PELOTON_MEMSET(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = INADDR_ANY;
  sin.sin_port = htons(port_);

  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);

  if (listen_fd_ < 0) {
    throw ConnectionException("Failed to create listen socket");
  }

  int conn_backlog = 12;
  int reuse = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  TrySslOperation<int, const struct sockaddr *, socklen_t>(
      bind, listen_fd_, (struct sockaddr *) &sin, sizeof(sin));
  TrySslOperation<int, int>(listen, listen_fd_, conn_backlog);

  dispatcher_task_ = std::make_shared<ConnectionDispatcherTask>(
      CONNECTION_THREAD_COUNT, listen_fd_);

  LOG_INFO("Listening on port %llu", (unsigned long long) port_);
  return *this;
}

void PelotonServer::ServerLoop() {
  if (settings::SettingsManager::GetBool(settings::SettingId::rpc_enabled)) {
    int rpc_port =
        settings::SettingsManager::GetInt(settings::SettingId::rpc_port);
    std::string address = "127.0.0.1:" + std::to_string(rpc_port);
    auto rpc_task = std::make_shared<PelotonRpcHandlerTask>(address.c_str());
    DedicatedThreadRegistry::GetInstance()
        .RegisterDedicatedThread<PelotonRpcHandlerTask>(this, rpc_task);
  }
  dispatcher_task_->EventLoop();

  peloton_close(listen_fd_);

  LOG_INFO("Server Closed");
}

void PelotonServer::Close() {
  LOG_INFO("Begin to stop server");
  dispatcher_task_->ExitLoop();
}

/**
 * Change port to new_port
 */
void PelotonServer::SetPort(int new_port) { port_ = new_port; }

}  // namespace network
}  // namespace peloton
