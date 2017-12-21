//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_worker_thread.h
//
// Identification: src/include/network/network_worker_thread.h
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

#include "common/exception.h"
#include "common/logger.h"
#include "container/lock_free_queue.h"
#include "network/notifiable_task.h"

// TODO(tianyu) Do we really want to hard code this?
#define QUEUE_SIZE 100

namespace peloton {
namespace network {

/**
 * A ConnectionHandlerTask is responsible for interacting with a client connection.
 *
 * A client connection, once taken by the dispatch, is sent to a handler. Then all related
 * client events are registered in the handler task. All client interaction happens on the
 * same ConnectionHandlerTask thread for the enitre lifetime of the connection.
 */
class ConnectionHandlerTask : public NotifiableTask {
  // TODO(tianyu) Maybe we don't need the callback on the CallbackUtil.
  // From an OOP perspective, the callback should be encapsulated in this class, and
  // we should only put callbacks with no clear owner in CallbackUtil
  friend class CallbackUtil;

public:
  /**
   * Constructs a new ConnecitonHandlerTask instance.
   * @param task_id task_id a unique id assigned to this task.
   */
  explicit ConnectionHandlerTask(int task_id);

  /**
   * @brief Notifies this ConnectionHandlerTask that a new client connection should be handled at socket fd.
   *
   * This method is meant to be invoked on another thread (probably the dispatcher) to update
   * the necessary data structure so the handler thread is woken up.
   *
   * @param conn_fd the client connection socket fd.
   */
  void Notify(int conn_fd);

private:
  // Notify new connection pipe(send end)
  int new_conn_send_fd_;

  // TODO(tianyu) Do we really need this queue, now that we are only sending an int?
  // The queue for new connection requests, elements represent the socket fd of new client connections
  LockFreeQueue<int> new_conn_queue_;
};

}  // namespace network
}  // namespace peloton
