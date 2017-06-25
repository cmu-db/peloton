//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// libevent_callbacks.cpp
//
// Implements Libevent callbacks for the protocol and their helpers
//
// Identification: src/wire/libevent_callbacks.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include "wire/libevent_server.h"
#include "common/macros.h"

namespace peloton {
namespace wire {

void WorkerHandleNewConn(evutil_socket_t new_conn_recv_fd,
                         UNUSED_ATTRIBUTE short ev_flags, void *arg) {
  // buffer used to receive messages from the main thread
  char m_buf[1];
  std::shared_ptr<NewConnQueueItem> item;
  LibeventSocket *conn;
  LibeventWorkerThread *thread = static_cast<LibeventWorkerThread *>(arg);

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
      conn = LibeventServer::GetConn(item->new_conn_fd);
      if (conn == nullptr) {
        LOG_DEBUG("Creating new socket fd:%d", item->new_conn_fd);
        /* create a new connection object */
        LibeventServer::CreateNewConn(item->new_conn_fd, item->event_flags,
                                      static_cast<LibeventThread *>(thread),
                                      CONN_READ);
      } else {
        LOG_DEBUG("Reusing socket fd:%d", item->new_conn_fd);
        /* otherwise reset and reuse the existing conn object */
        conn->Reset();
        conn->Init(item->event_flags, static_cast<LibeventThread *>(thread),
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
  LibeventSocket *conn = static_cast<LibeventSocket *>(arg);
  PL_ASSERT(conn != nullptr);
  conn->event_flags = ev_flags;
  PL_ASSERT(connfd == conn->sock_fd);
  StateMachine(conn);
}

void StateMachine(LibeventSocket *conn) {
  bool done = false;

  while (done == false) {
    LOG_INFO("current state: %d", conn->state);
    switch (conn->state) {
      case CONN_LISTENING: {
        struct sockaddr_storage addr;
        socklen_t addrlen = sizeof(addr);
        int new_conn_fd =
            accept(conn->sock_fd, (struct sockaddr *)&addr, &addrlen);
        if (new_conn_fd == -1) {
          LOG_ERROR("Failed to accept");
        }
        (static_cast<LibeventMasterThread *>(conn->thread))
            ->DispatchConnection(new_conn_fd, EV_READ | EV_PERSIST);
        done = true;
        break;
      }

      case CONN_READ: {
        auto res = conn->FillReadBuffer();
        switch (res) {
          case READ_DATA_RECEIVED:
            // wait for some other event
            conn->TransitState(CONN_PROCESS);
            break;

          case READ_NO_DATA_RECEIVED:
            // process what we read
            conn->TransitState(CONN_WAIT);
            break;

          case READ_ERROR:
            // fatal error for the connection
            conn->TransitState(CONN_CLOSING);
            break;
        }
        break;
      }

      case CONN_WAIT: {
        if (conn->UpdateEvent(EV_READ | EV_PERSIST) == false) {
          LOG_ERROR("Failed to update event, closing");
          conn->TransitState(CONN_CLOSING);
          break;
        }

        conn->TransitState(CONN_READ);
        done = true;
        break;
      }

      case CONN_PROCESS: {
        bool status;

        if(conn->pkt_manager.ssl_sent) {
            // start SSL handshake
            // TODO: consider free conn_SSL_context
            conn->conn_SSL_context = SSL_new(LibeventServer::ssl_context);
            if (SSL_set_fd(conn->conn_SSL_context, conn->sock_fd) == 0) {
              LOG_ERROR("Failed to set SSL fd");
              PL_ASSERT(false);
            }
            int ssl_accept_ret;
            if ((ssl_accept_ret = SSL_accept(conn->conn_SSL_context)) <= 0) {
              LOG_ERROR("Failed to accept (handshake) client SSL context.");
              LOG_ERROR("ssl error: %d", SSL_get_error(conn->conn_SSL_context, ssl_accept_ret));
              // TODO: consider more about proper action
              PL_ASSERT(false);
              conn->TransitState(CONN_CLOSED);
            }
            LOG_ERROR("SSL handshake completed");
            conn->pkt_manager.ssl_sent = false;
        }

        if (conn->rpkt.header_parsed == false) {
          // parse out the header first
          if (conn->ReadPacketHeader() == false) {
            // need more data
            conn->TransitState(CONN_WAIT);
            break;
          }
        }
        PL_ASSERT(conn->rpkt.header_parsed == true);

        if (conn->rpkt.is_initialized == false) {
          // packet needs to be initialized with rest of the contents
          if (conn->ReadPacket() == false) {
            // need more data
            conn->TransitState(CONN_WAIT);
            break;
          }
        }
        PL_ASSERT(conn->rpkt.is_initialized == true);

        if (conn->pkt_manager.is_started == false) {
          // We need to handle startup packet first
          int status_res = conn->pkt_manager.ProcessInitialPacket(&conn->rpkt);
          status = (status_res != 0);
          if (status_res == 1) {
            conn->pkt_manager.is_started = true;
          }
          else if (status_res == -1){
            conn->pkt_manager.ssl_sent = true;
          }
        } else {
          // Process all other packets
          status = conn->pkt_manager.ProcessPacket(&conn->rpkt,
                                                   (size_t)conn->thread_id);
        }

        if (status == false) {
          // packet processing can't proceed further
          conn->TransitState(CONN_CLOSING);
        } else {
          // We should have responses ready to send
          conn->TransitState(CONN_WRITE);
        }
        break;
      }

      case CONN_WRITE: {
        // examine write packets result
        switch (conn->WritePackets()) {
          case WRITE_COMPLETE: {
            // Input Packet can now be reset, before we parse the next packet
            conn->rpkt.Reset();
            conn->UpdateEvent(EV_READ | EV_PERSIST);
            conn->TransitState(CONN_PROCESS);
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
            conn->TransitState(CONN_CLOSING);
            break;
          }
        }
        break;
      }

      case CONN_CLOSING: {
        conn->CloseSocket();
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
  LOG_INFO("stop");
  event_base_loopexit(base, NULL);
}

void ControlCallback::ServerControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t
                                                 fd,
                                             UNUSED_ATTRIBUTE short what,
                                             void *arg) {
  LibeventServer *server = (LibeventServer *)arg;
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
  LibeventWorkerThread *thread = static_cast<LibeventWorkerThread *>(arg);
  if (!thread->GetThreadIsStarted()) {
    thread->SetThreadIsStarted(true);
  }
  if (thread->GetThreadIsClosed()) {
    event_base_loopexit(thread->GetEventBase(), NULL);
  }
}
}
}
