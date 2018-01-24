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

namespace peloton {
namespace network {
class NetworkManager;

void CallbackUtil::WorkerHandleNewConn(evutil_socket_t new_conn_recv_fd,
                         UNUSED_ATTRIBUTE short ev_flags, void *arg) {
  // buffer used to receive messages from the main thread
  char m_buf[1];
  std::shared_ptr<NewConnQueueItem> item;
  NetworkConnection *conn;
  NetworkWorkerThread *thread = static_cast<NetworkWorkerThread *>(arg);

  // pipe fds should match
  PL_ASSERT(new_conn_recv_fd == thread->GetNewConnReceiveFd());

  // read the operation that needs to be performed
  if (read(new_conn_recv_fd, m_buf, 1) != 1) {
    LOG_ERROR("Can't read from the libevent pipe");
    return;
  }

  switch (m_buf[0]) {
    /* new connection case */
    case 'c': {
      // fetch the new connection fd from the queue
      thread->new_conn_queue.Dequeue(item);
      conn = NetworkManager::GetConnection(item->new_conn_fd);
      if (conn == nullptr) {
        LOG_DEBUG("Creating new socket fd:%d", item->new_conn_fd);
        /* create a new connection object */
        NetworkManager::CreateNewConnection(item->new_conn_fd, item->event_flags,
                                            static_cast<NetworkThread *>(thread),
                                            ConnState::CONN_READ);
      } else {
        LOG_DEBUG("Reusing connection object with socket fd:%d", item->new_conn_fd);
        /* otherwise reset and reuse the existing conn object */
        conn->Reset();
        conn->Init(item->event_flags, static_cast<NetworkThread *>(thread),
                   ConnState::CONN_READ);
      }
      break;
    }

    default:
      LOG_ERROR("Unexpected message. Shouldn't reach here");
  }
}

void CallbackUtil::EventHandler(UNUSED_ATTRIBUTE evutil_socket_t connfd,
                                short ev_flags, void *arg) {
  LOG_TRACE("Event callback fired for connfd: %d", connfd);
  NetworkConnection *conn = static_cast<NetworkConnection *>(arg);
  PL_ASSERT(conn != nullptr);
  if (connfd > 0) {
    conn->event_flags = ev_flags;
  }

#ifdef LOG_DEBUG_ENABLED
  if (ev_flags == EV_READ)
    assert(connfd == conn->sock_fd);
#endif
  NetworkConnection::StateMachine(conn);
}

/**
 * Stop signal handling
 */
void CallbackUtil::Signal_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                                      UNUSED_ATTRIBUTE short what, void *arg) {
  struct event_base *base = (event_base *)arg;
  LOG_TRACE("stop");
  event_base_loopexit(base, NULL);
}

void CallbackUtil::ServerControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t
                                                 fd,
                                             UNUSED_ATTRIBUTE short what,
                                             void *arg) {
  NetworkManager *server = (NetworkManager *)arg;
  if (server->GetIsStarted() == false) {
    server->SetIsStarted(true);
  }
  LOG_TRACE("Closing server::exiting event loop -- Start");
  if (server->GetIsClosed() == true) {
    event_base_loopexit(server->GetEventBase(), NULL);
  }
  LOG_TRACE("Closing server::exiting event loop -- Done");
}

void CallbackUtil::ThreadControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t
                                                 fd,
                                             UNUSED_ATTRIBUTE short what,
                                             void *arg) {
  NetworkWorkerThread *thread = static_cast<NetworkWorkerThread *>(arg);
  if (!thread->GetThreadIsStarted()) {
    thread->SetThreadIsStarted(true);
  }
  if (thread->GetThreadIsClosed()) {
    event_base_loopexit(thread->GetEventBase(), NULL);
  }
}
}
}
