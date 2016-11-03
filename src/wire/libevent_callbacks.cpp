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
  PL_ASSERT(new_conn_recv_fd == thread->new_conn_receive_fd);

  LOG_DEBUG("TID: %d", thread->GetThreadID());
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
        /* create a new connection object */
        LibeventServer::CreateNewConn(item->new_conn_fd, item->event_flags,
                                      static_cast<LibeventThread *>(thread),
                                      CONN_READ);
      } else {
        /* otherwise reset and reuse the existing conn object */
        conn->Reset(item->event_flags, static_cast<LibeventThread *>(thread),
                    CONN_READ);
      }
      break;
    }

    default:
      LOG_ERROR("Unexpected message. Shouldn't reach here");
  }
}

void EventHandler(evutil_socket_t connfd, short ev_flags, void *arg) {
  LOG_INFO("Event callback fired for connfd: %d", connfd);
  LibeventSocket *conn = static_cast<LibeventSocket *>(arg);
  PL_ASSERT(conn != nullptr);
  conn->event_flags = ev_flags;
  PL_ASSERT(connfd == conn->sock_fd);
  StateMachine(conn);
}

void StateMachine(LibeventSocket *conn) {
  LOG_DEBUG("StateMachine invoked");
  bool done = false;

  while (done == false) {
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
        char buf[100];
        ssize_t bytes_read = read(conn->sock_fd, buf, 100);
        if (bytes_read > 0) {
          std::unique_ptr<Packet> response(new Packet());
          response->msg_type = PARSE_COMPLETE;
          PacketPutString(response, "dummy string to test");
          conn->responses.push_back(std::move(response));

          if (conn->WritePackets(true) == false) {
            TransitState(conn, CONN_WRITE);
            UpdateEvent(conn, EV_WRITE | EV_PERSIST);
          }
        } else {
          // should also check error code
          TransitState(conn, CONN_WAIT);
        }
        //
        // if bytes_read doesn't meet expected size, then go to wait state
        // if we have enough bytes read, then process packet,
        //   try write to local buffer
        //   if local buffer doesn't have enough space or it's sync message
        // then
        //		try flush
        //      if socket is not ready for flush, then to go write state
        //   continue reading

        // bool force_flush = false;
        break;
        // done = true;
      }

      case CONN_WRITE: {
        // If the socket is still not ready, remain in CONN_WRITE state
        if (conn->WritePackets(true) == false) {
          // do nothing. remain in write state
        } else {
          // transit to read state
          UpdateEvent(conn, EV_READ | EV_PERSIST);
          TransitState(conn, CONN_READ);
        }
        done = true;
        break;
      }
      case CONN_WAIT: {
        UpdateEvent(conn, EV_READ | EV_PERSIST);
        TransitState(conn, CONN_READ);
        done = true;
        break;
      }
      case CONN_CLOSING: {
        LOG_INFO("Close is not implemented yet");
        done = true;
        break;
      }
      // TODO handle other states, too
      default: {}
    }
  }
}

void TransitState(LibeventSocket *conn, ConnState next_state) {
  PL_ASSERT(conn != nullptr);
  conn->state = next_state;
  LOG_TRACE("conn %d transit to state %d", conn->sock_fd, (int)next_state);
}

// Update event
void UpdateEvent(LibeventSocket *conn, short flags) {
  PL_ASSERT(conn != nullptr);
  auto base = conn->thread->GetEventBase();
  auto event = conn->event;
  auto result =
      event_assign(event, base, conn->sock_fd, flags, EventHandler, conn);
  if (result != 0) {
    LOG_ERROR("Failed to update event");
  }
}
}
}
