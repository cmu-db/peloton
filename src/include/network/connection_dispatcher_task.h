//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_master_thread.h
//
// Identification: src/include/network/network_thread.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "notifiable_task.h"
#include "network_state.h"
#include "concurrency/epoch_manager_factory.h"
#include "connection_handler_task.h"
#include "network_manager.h"
namespace peloton {
namespace network {

// Forward Declarations
class ConnectionHandlerTask;

// a master thread contains multiple worker threads.
class ConnectionDispatcherTask : public NotifiableTask {
 private:
  const int num_threads_;

  // TODO: have a smarter dispatch scheduler
  std::atomic<int> next_thread_id_;  // next thread we dispatched to

 public:
  ConnectionDispatcherTask(const int num_threads, struct event_base *libevent_base);

  void Start();

  void Stop();

  void DispatchConnection(int new_conn_fd, short event_flags);

  std::vector<std::shared_ptr<ConnectionHandlerTask>> &GetWorkerThreads();

  static void StartWorker(peloton::network::ConnectionHandlerTask *worker_thread);
};

}  // namespace network
}  // namespace peloton
