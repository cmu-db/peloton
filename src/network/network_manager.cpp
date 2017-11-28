//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_manager.cpp
//
// Identification: src/network/network_manager.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "event2/thread.h"
#include <fstream>

#include "network/network_manager.h"

#include "settings/settings_manager.h"
#include "peloton_config.h"

namespace peloton {
namespace network {

int NetworkManager::recent_connfd = -1;
SSL_CTX *NetworkManager::ssl_context = nullptr;
std::string NetworkManager::private_key_file_;
std::string NetworkManager::certificate_file_;
std::string NetworkManager::root_cert_file_;
SSLLevel NetworkManager::ssl_level_;

std::unordered_map<int, std::unique_ptr<NetworkConnection>> &
NetworkManager::GetGlobalSocketList() {
  // mapping from socket id to socket object.
  static std::unordered_map<int, std::unique_ptr<NetworkConnection>>
      global_socket_list;

  return global_socket_list;
}

NetworkConnection *NetworkManager::GetConnection(const int &connfd) {
  auto &global_socket_list = GetGlobalSocketList();
  if (global_socket_list.find(connfd) != global_socket_list.end()) {
    return global_socket_list.at(connfd).get();
  } else {
    return nullptr;
  }
}

void NetworkManager::CreateNewConnection(const int &connfd, short ev_flags,
                                         NetworkThread *thread,
                                         ConnState init_state) {
  auto &global_socket_list = GetGlobalSocketList();
  recent_connfd = connfd;
  if (global_socket_list.find(connfd) == global_socket_list.end()) {
    LOG_INFO("Create new connection: id = %d", connfd);
  }
  bool ssl_able = (GetSSLLevel() != SSLLevel::SSL_DISABLE);
  global_socket_list[connfd].reset(
      new NetworkConnection(connfd, ev_flags, thread, init_state, ssl_able));
  thread->SetThreadSockFd(connfd);
}

void NetworkManager::LoadSSLFileSettings() {
  private_key_file_ = DATA_DIR + settings::SettingsManager::GetString(settings::SettingId::private_key_file);
  certificate_file_ = DATA_DIR + settings::SettingsManager::GetString(settings::SettingId::certificate_file);
  root_cert_file_ = DATA_DIR + settings::SettingsManager::GetString(settings::SettingId::root_cert_file);
}

void NetworkManager::SSLInit() {

  if (!settings::SettingsManager::GetBool(settings::SettingId::ssl)) {
    SetSSLLevel(SSLLevel::SSL_DISABLE);
    return;
  }

  SetSSLLevel(SSLLevel::SSL_VERIIFY);

  /* Initialize SSL listener connection */
  SSL_load_error_strings();
  SSL_library_init();

  if ((ssl_context = SSL_CTX_new(SSLv23_method())) == nullptr) {
    SetSSLLevel(SSLLevel::SSL_DISABLE);
    return;
  }

  if (SSL_CTX_load_verify_locations(ssl_context, certificate_file_.c_str(), nullptr) != 1) {
    LOG_ERROR("Exception when loading root_crt!");
    SetSSLLevel(SSLLevel::SSL_PREFER);
  }

  if (SSL_CTX_set_default_verify_paths(ssl_context) != 1) {
    LOG_ERROR("Exception when setting default verify path!");
    SetSSLLevel(SSLLevel::SSL_PREFER);
  }

  LOG_INFO("certificate file path %s", certificate_file_.c_str());
  if (SSL_CTX_use_certificate_chain_file(ssl_context, certificate_file_.c_str()) != 1) {
    SSL_CTX_free(ssl_context);
    LOG_ERROR("Exception when loading server certificate!");
    ssl_context = nullptr;
    SetSSLLevel(SSLLevel::SSL_DISABLE);
    return;
  }

  LOG_INFO("private key file path %s", private_key_file_.c_str());
  // register private key
  if (SSL_CTX_use_PrivateKey_file(ssl_context, private_key_file_.c_str(), SSL_FILETYPE_PEM) != 1) {
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
    SSL_CTX_set_verify(ssl_context, SSL_VERIFY_PEER, verify_callback);
    SSL_CTX_set_verify_depth(ssl_context, 4);
  } else {
    SSL_CTX_set_verify(ssl_context, SSL_VERIFY_NONE, verify_callback);
  }
  // postgres use : SSL_OP_SINGLE_DH_USE
  SSL_CTX_set_options(ssl_context, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
}

NetworkManager::NetworkManager() {
  evthread_use_pthreads();
  base_ = event_base_new();
  evthread_make_base_notifiable(base_);

  // Create our event base
  if (!base_) {
    throw ConnectionException("Couldn't open event base");
  }

  // Add hang up signal event
  ev_stop_ =
      evsignal_new(base_, SIGHUP, CallbackUtil::Signal_Callback, base_);
  evsignal_add(ev_stop_, NULL);

  // Add timeout event to check server's start/close flag every one second
  struct timeval one_seconds = {1, 0};
  ev_timeout_ = event_new(base_, -1, EV_TIMEOUT | EV_PERSIST,
                          CallbackUtil::ServerControl_Callback, this);
  event_add(ev_timeout_, &one_seconds);

  // a master thread is responsible for coordinating worker threads.
  master_thread_ =
      std::make_shared<NetworkMasterThread>(CONNECTION_THREAD_COUNT, base_);

  port_ = settings::SettingsManager::GetInt(settings::SettingId::port);
  max_connections_ = settings::SettingsManager::GetInt(settings::SettingId::max_connections);

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

int NetworkManager::verify_callback(int ok, X509_STORE_CTX *store) {
  char data[256];
  if (!ok)
  {
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

void NetworkManager::StartServer() {
  if (settings::SettingsManager::GetString(settings::SettingId::socket_family) == "AF_INET") {
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

    int conn_backlog = 12;
    int reuse = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    if (bind(listen_fd, (struct sockaddr *) &sin, sizeof(sin)) < 0)
    {
      if (GetSSLLevel() != SSLLevel::SSL_DISABLE) {
        SSL_CTX_free(ssl_context);
      }
      throw ConnectionException("Failed binding socket.");
    }

    if (listen(listen_fd, conn_backlog) < 0)
    {
      if (GetSSLLevel() != SSLLevel::SSL_DISABLE) {
        SSL_CTX_free(ssl_context);
      }
      throw ConnectionException("Error listening onsocket.");
    }

    master_thread_->Start();

    NetworkManager::CreateNewConnection(listen_fd, EV_READ | EV_PERSIST,
                                        master_thread_.get(), ConnState::CONN_LISTENING);

    LOG_INFO("Listening on port %llu", (unsigned long long) port_);
    event_base_dispatch(base_);
    LOG_INFO("Closing server");
    NetworkManager::GetConnection(listen_fd)->CloseSocket();

    // Free events and event base
    event_free(NetworkManager::GetConnection(listen_fd)->network_event);
    event_free(NetworkManager::GetConnection(listen_fd)->workpool_event);
    event_free(ev_stop_);
    event_free(ev_timeout_);
    event_base_free(base_);

    master_thread_->Stop();
    LOG_INFO("Server Closed");
  }

  // This socket family code is not implemented yet
  else {
    throw ConnectionException("Unsupported socket family");
  }
}

void NetworkManager::CloseServer() {
  LOG_INFO("Begin to stop server");
  this->SetIsClosed(true);
}

/**
 * Change port to new_port
 */
void NetworkManager::SetPort(int new_port) { port_ = new_port; }

}  // namespace network
}  // namespace peloton
