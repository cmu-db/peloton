//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_handler_task.h
//
// Identification: src/include/network/connection_handler_task.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>

#include <unistd.h>

#include "common/exception.h"
#include "common/logger.h"
#include "common/container/lock_free_queue.h"
#include "common/notifiable_task.h"

namespace peloton {
namespace network {

/**
 * A ConnectionHandlerTask is responsible for interacting with a client
 * connection.
 *
 * A client connection, once taken by the dispatch, is sent to a handler. Then
 * all related
 * client events are registered in the handler task. All client interaction
 * happens on the
 * same ConnectionHandlerTask thread for the enitre lifetime of the connection.
 */
class ConnectionHandlerTask : public NotifiableTask {
 public:
  /**
   * Constructs a new ConnectionHandlerTask instance.
   * @param task_id task_id a unique id assigned to this task.
   */
  explicit ConnectionHandlerTask(int task_id);

  /**
   * @brief Notifies this ConnectionHandlerTask that a new client connection
   * should be handled at socket fd.
   *
   * This method is meant to be invoked on another thread (probably the
   * dispatcher) to update
   * the necessary data structure so the handler thread is woken up.
   *
   * @param conn_fd the client connection socket fd.
   */
  void Notify(int conn_fd);

  /**
   * @brief Handles a new client assigned to this handler by the dispatcher.
   *
   * This method will create the necessary data structure for the client and
   * register its event base to receive updates with appropriate callbacks
   * when the client writes to the socket.
   *
   * @param new_conn_recv_fd the socket fd of the new connection
   * @param flags unused. For compliance with libevent callback interface.
   */
  void HandleDispatch(int new_conn_recv_fd, short flags);

 private:
  // Notify new connection pipe(send end)
  int new_conn_send_fd_;
};

}  // namespace network
}  // namespace peloton
