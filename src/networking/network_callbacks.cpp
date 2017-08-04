//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_callbacks.cpp
//
// Implements Libevent callbacks for the protocol and their helpers
//
// Identification: src/networking/network_callbacks.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include "networking/network_manager.h"
#include "networking/network_connection.h"
#include "common/macros.h"

namespace peloton {
namespace networking {

void WorkerHandleNewConn(evutil_socket_t new_conn_recv_fd,
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
      conn = NetworkManager::GetConn(item->new_conn_fd);
      if (conn == nullptr) {
        LOG_DEBUG("Creating new socket fd:%d", item->new_conn_fd);
        /* create a new connection object */
        NetworkManager::CreateNewConn(item->new_conn_fd, item->event_flags,
                                      static_cast<NetworkThread *>(thread),
                                      CONN_READ);
      } else {
        LOG_DEBUG("Reusing socket fd:%d", item->new_conn_fd);
        /* otherwise reset and reuse the existing conn object */
        conn->Reset();
        conn->Init(item->event_flags, static_cast<NetworkThread *>(thread),
                   CONN_READ);
      }
      break;
    }

    default:
      LOG_ERROR("Unexpected message. Shouldn't reach here");
  }
}

void EventHandler(UNUSED_ATTRIBUTE evutil_socket_t connfd, short ev_flags,
                  void *arg) {
  LOG_TRACE("Event callback fired for connfd: %d", connfd);
  NetworkConnection *conn = static_cast<NetworkConnection *>(arg);
  PL_ASSERT(conn != nullptr);
  conn->event_flags = ev_flags;
  PL_ASSERT(connfd == conn->sock_fd);
  StateMachine(conn);
}

void StateMachine(NetworkConnection *network_connection) {
  bool done = false;

  while (done == false) {
    LOG_TRACE("current state: %d", network_manager->state);
    switch (network_connection->state) {
      case CONN_LISTENING: {
        struct sockaddr_storage addr;
        socklen_t addrlen = sizeof(addr);
        int new_conn_fd =
            accept(network_connection->sock_fd, (struct sockaddr *)&addr, &addrlen);
        if (new_conn_fd == -1) {
          LOG_ERROR("Failed to accept");
        }
        (static_cast<NetworkMasterThread *>(network_connection->thread))
            ->DispatchConnection(new_conn_fd, EV_READ | EV_PERSIST);
        done = true;
        break;
      }

      case CONN_READ: {
        auto res = network_connection->FillReadBuffer();
        switch (res) {
          case READ_DATA_RECEIVED:
            // wait for some other event
            network_connection->TransitState(CONN_PARSE_PACKET);
            break;

          case READ_NO_DATA_RECEIVED:
            // process what we read
            network_connection->TransitState(CONN_WAIT);
            break;

          case READ_ERROR:
            // fatal error for the connection
            network_connection->TransitState(CONN_CLOSING);
            break;
        }
        break;
      }

      case CONN_WAIT: {
        if (network_connection->UpdateEvent(EV_READ | EV_PERSIST) == false) {
          LOG_ERROR("Failed to update event, closing");
          network_connection->TransitState(CONN_CLOSING);
          break;
        }

        network_connection->TransitState(CONN_READ);
        done = true;
        break;
      }

      case CONN_PARSE_PACKET: {
        bool status;

        if(network_connection->protocol_handler_.ssl_sent) {
            // start SSL handshake
            // TODO: consider free conn_SSL_context
          network_connection->conn_SSL_context = SSL_new(NetworkManager::ssl_context);
            if (SSL_set_fd(network_connection->conn_SSL_context, network_connection->sock_fd) == 0) {
              LOG_ERROR("Failed to set SSL fd");
              PL_ASSERT(false);
            }
            int ssl_accept_ret;
            if ((ssl_accept_ret = SSL_accept(network_connection->conn_SSL_context)) <= 0) {
              LOG_ERROR("Failed to accept (handshake) client SSL context.");
              LOG_ERROR("ssl error: %d", SSL_get_error(network_connection->conn_SSL_context, ssl_accept_ret));
              // TODO: consider more about proper action
              PL_ASSERT(false);
              network_connection->TransitState(CONN_CLOSED);
            }
            LOG_ERROR("SSL handshake completed");
            network_connection->protocol_handler_.ssl_sent = false;
        }

        if (network_connection->protocol_handler_ == nullptr) {
          // We need to handle startup packet first
          int status_res = network_connection->ProcessInitialPacket();
          status = (status_res != 0);
          if (status_res == 1) {
            network_connection->protocol_handler_.is_started = true;
          } else if (status_res == -1) {
            network_connection->protocol_handler_.ssl_sent = true;
          }
        }

        // Parse rbuf into packets
        // When the session is end

        // When the packet is not done

        /*
        if (network_manager->rpkt.header_parsed == false) {
          // parse out the header first
          if (network_manager->ReadPacketHeader() == false) {
            // need more data
            network_manager->TransitState(CONN_WAIT);
            break;
          }
        }
        PL_ASSERT(network_manager->rpkt.header_parsed == true);

        if (network_manager->rpkt.is_initialized == false) {
          // packet needs to be initialized with rest of the contents
          if (network_manager->ReadPacket() == false) {
            // need more data
            network_manager->TransitState(CONN_WAIT);
            break;
          }
        }
        PL_ASSERT(network_manager->rpkt.is_initialized == true);
*/
        break;
      }

      case CONN_PROCESS: {
        bool status;

      }

      case CONN_WRITE: {
        // examine write packets result
        switch (network_connection->WritePackets()) {
          case WRITE_COMPLETE: {
            // Input Packet can now be reset, before we parse the next packet
            network_connection->UpdateEvent(EV_READ | EV_PERSIST);
            network_connection->TransitState(CONN_PROCESS);
            break;
          }

          case WRITE_NOT_READY: {
            // we can't write right now. Exit state machine
            // and wait for next callback
            done = true;
            break;
          }

          case WRITE_ERROR: {
            LOG_ERROR("Error during write, closing connection");
            network_connection->TransitState(CONN_CLOSING);
            break;
          }
        }
        break;
      }

      case CONN_CLOSING: {
        network_connection->CloseSocket();
        done = true;
        break;
      }

      case CONN_CLOSED: {
        done = true;
        break;
      }

      case CONN_INVALID: {
        PL_ASSERT(false);
        break;
      }
    }
  }
}

/**
 * Stop signal handling
 */
void ControlCallback::Signal_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                                      UNUSED_ATTRIBUTE short what, void *arg) {
  struct event_base *base = (event_base *)arg;
  LOG_TRACE("stop");
  event_base_loopexit(base, NULL);
}

void ControlCallback::ServerControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t
                                                 fd,
                                             UNUSED_ATTRIBUTE short what,
                                             void *arg) {
  NetworkManager *server = (NetworkManager *)arg;
  if (server->GetIsStarted() == false) {
    server->SetIsStarted(true);
  }
  if (server->GetIsClosed() == true) {
    event_base_loopexit(server->GetEventBase(), NULL);
  }
}

void ControlCallback::ThreadControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t
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
