//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_dispatcher_task.h
//
// Identification: src/include/network/connection_dispatcher_task.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/notifiable_task.h"
#include "network_state.h"
#include "concurrency/epoch_manager_factory.h"
#include "connection_handler_task.h"

namespace peloton {
namespace network {

/**
 * @brief A ConnectionDispatcherTask on the main server thread and dispatches
 * incoming connections to handler threads.
 *
 * On construction, the dispatcher also spawns a number of handlers running on
 * their own threads. The dispatcher is
 * then responsible for maintain, and when shutting down, shutting down the
 * spawned handlers also.
 */
class ConnectionDispatcherTask : public NotifiableTask {
 public:
  /**
   * Creates a new ConnectionDispatcherTask, spawning the specified number of
   * handlers, each running on their own threads.
   *
   * @param num_handlers The number of handler tasks to spawn.
   * @param listen_fd The server socket fd to listen on.
   */
  ConnectionDispatcherTask(int num_handlers, int listen_fd);

  /**
   * @brief Dispatches the client connection at fd to a handler.
   * Currently, the dispatch uses round-robin, and thread communication is
   * achieved
   * through channels. The dispatch writes a symbol to the fd that the handler
   * is configured
   * to receive updates on.
   *
   * @param fd the socket fd of the client connection being dispatched
   * @param flags Unused. This is here to conform to libevent callback function
   * signature.
   */
  void DispatchConnection(int fd, short flags);

  /**
   * Breaks the dispatcher and managed handlers from their event loops.
   */
  void ExitLoop() override;

 private:
  std::vector<std::shared_ptr<ConnectionHandlerTask>> handlers_;
  // TODO: have a smarter dispatch scheduler, we currently use round-robin
  std::atomic<int> next_handler_;
};

}  // namespace network
}  // namespace peloton
