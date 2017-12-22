//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_dispatcher_task.h
//
// Identification: src/include/network/connection_dispatcher_task.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "notifiable_task.h"
#include "network_state.h"
#include "concurrency/epoch_manager_factory.h"
#include "connection_handler_task.h"

namespace peloton {
namespace network {

// a master thread contains multiple worker threads.
class ConnectionDispatcherTask : public NotifiableTask {
public:
  /**
   * Creates a new ConnectionDispatcherTask, spawning the specified number of handlers, each running on their own thread.
   * @param num_handlers the number of handler tasks to spawn
   */
  explicit ConnectionDispatcherTask(int num_handlers);

  // TODO(tianyu) Maybe do this at destruction time?
  void Stop();

  /**
   * @brief Dispatches the client connection at fd to a handler.
   * Currently, the dispatch uses round-robin, and thread communication is achieved
   * through channels. The dispatch writes a symbol to the fd that the handler is configured
   * to receive updates on.
   *
   * @param fd the socket fd of the client connection being dispatched
   */
  void DispatchConnection(int fd);

private:
  std::vector<std::shared_ptr<ConnectionHandlerTask>> handlers_;
  // TODO: have a smarter dispatch scheduler, we currently use round-robin
  std::atomic<int> next_handler_;  // next thread we dispatched to
};

}  // namespace network
}  // namespace peloton
