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
#include "notifiable_task.h"

#define QUEUE_SIZE 100

namespace peloton {
namespace network {

class ConnectionHandlerTask : public NotifiableTask {
 private:
  // Notify new connection pipe(send end)
  int new_conn_send_fd_, new_conn_receive_fd;

 public:
  /* The queue for new connection requests */
  LockFreeQueue<std::shared_ptr<NewConnQueueItem>> new_conn_queue;

 public:
  explicit ConnectionHandlerTask(int task_id);

  // TODO(tianyu): see if we need any additional options
  void Notify(int conn_fd);
};

}  // namespace network
}  // namespace peloton
