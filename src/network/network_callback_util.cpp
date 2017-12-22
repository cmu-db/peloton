//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_callback_util.cpp
//
// Implements Libevent callbacks for the protocol and their helpers
//
// Identification: src/network/network_callbacks_util.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/network_callback_util.h"
#include "network/connection_handle_factory.h"

namespace peloton {
namespace network {

void CallbackUtil::OnNewConnection(evutil_socket_t fd, short, void *arg) {
  static_cast<ConnectionDispatcherTask *>(arg)->DispatchConnection(fd);
}

void CallbackUtil::OnNewConnectionDispatch(evutil_socket_t new_conn_recv_fd,
                                           UNUSED_ATTRIBUTE short ev_flags, void *arg) {
  // buffer used to receive messages from the main thread
  char m_buf[1];
  int client_fd;
  std::shared_ptr<ConnectionHandle> conn;
  auto *handler = static_cast<ConnectionHandlerTask *>(arg);

  // read the operation that needs to be performed
  if (read(new_conn_recv_fd, m_buf, 1) != 1) {
    LOG_ERROR("Can't read from the libevent pipe");
    return;
  }

  switch (m_buf[0]) {
    /* new connection case */
    case 'c': {
      // fetch the new connection fd from the queue
      handler->new_conn_queue_.Dequeue(client_fd);
      conn = ConnectionHandleFactory::GetInstance().GetConnectionHandle(client_fd, handler);
      break;
    }

    default:
      LOG_ERROR("Unexpected message. Shouldn't reach here");
  }
}


void CallbackUtil::OnNetworkEvent(evutil_socket_t, short, void *arg) {
  static_cast<ConnectionHandle *>(arg)->TriggerStateMachine();
}

/**
 * Stop signal handling
 */
void CallbackUtil::OnSighup(evutil_socket_t, short, void *arg) {
    static_cast<NotifiableTask *>(arg)->Break();
}

// TODO(tianyu): We probably don't need these
void CallbackUtil::ServerControl_Callback(evutil_socket_t, short, void *arg) {
  NetworkManager *server = (NetworkManager *)arg;
  if (server->GetIsStarted() == false) {
    server->SetIsStarted(true);
  }
  LOG_TRACE("Closing server::exiting event loop -- Start");
  if (server->GetIsClosed() == true) {
    server->Break();
  }
  LOG_TRACE("Closing server::exiting event loop -- Done");
}

void CallbackUtil::ThreadControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t
                                                 fd,
                                             UNUSED_ATTRIBUTE short what,
                                             void *arg) {
  ConnectionHandlerTask *handler = static_cast<ConnectionHandlerTask *>(arg);
  if (!handler->GetThreadIsStarted()) {
    handler->SetThreadIsStarted(true);
  }
  if (handler->GetThreadIsClosed()) {
    handler->Break();
  }
}
}
}
