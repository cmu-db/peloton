///===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_callback_util.h
//
// Identification: src/include/network/network_callback_util.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <unistd.h>

#include "common/logger.h"
#include "common/exception.h"
#include "network_state.h"
#include "network_worker_thread.h"
#include "network_connection.h"

namespace peloton {
namespace network {
/*
 * CallbackUtil - Some callback helper functions
 */
class CallbackUtil {
 public:
  /* Used by a worker thread to receive a new connection from the main thread and
   * launch the event handler */
  static void WorkerHandleNewConn(evutil_socket_t new_conn_recv_fd,
                         UNUSED_ATTRIBUTE short ev_flags, void *arg);

  /* Used when a write action is happening in one connection */
  static void EventHandler(UNUSED_ATTRIBUTE evutil_socket_t connfd,
                                short ev_flags, void *arg);

  /* Used to handle signals */
  static void Signal_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                              UNUSED_ATTRIBUTE short what, void *arg);

  /* Used to control server start and close */
  static void ServerControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                                     UNUSED_ATTRIBUTE short what, void *arg);

  /* Used to control thread event loop's begin and exit */
  static void ThreadControl_Callback(UNUSED_ATTRIBUTE evutil_socket_t fd,
                                     UNUSED_ATTRIBUTE short what, void *arg);

};

}
}
