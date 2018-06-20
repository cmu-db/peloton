//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_dispatcher_task.cpp
//
// Identification: src/network/connection_dispatcher_task.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/connection_dispatcher_task.h"

#define MASTER_THREAD_ID (-1)

namespace peloton {
namespace network {

ConnectionDispatcherTask::ConnectionDispatcherTask(int num_handlers,
                                                   int listen_fd)
    : NotifiableTask(MASTER_THREAD_ID), next_handler_(0) {
  RegisterEvent(
      listen_fd, EV_READ | EV_PERSIST,
      METHOD_AS_CALLBACK(ConnectionDispatcherTask, DispatchConnection), this);
  RegisterSignalEvent(SIGHUP, METHOD_AS_CALLBACK(NotifiableTask, ExitLoop),
                      this);

  // TODO(tianyu) Figure out what this initialization logic is doing and
  // potentially rewrite
  // register thread to epoch manager.
  if (concurrency::EpochManagerFactory::GetEpochType() ==
      EpochType::DECENTRALIZED_EPOCH) {
    for (size_t task_id = 0; task_id < (size_t)num_handlers; task_id++) {
      concurrency::EpochManagerFactory::GetInstance().RegisterThread(task_id);
    }
  }

  // create worker threads.
  for (int task_id = 0; task_id < num_handlers; task_id++) {
    auto handler = std::make_shared<ConnectionHandlerTask>(task_id);
    handlers_.push_back(handler);
    thread_pool.SubmitDedicatedTask([=] { handler->EventLoop(); });
  }
}

void ConnectionDispatcherTask::DispatchConnection(int fd, short) {
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof(addr);

  int new_conn_fd = accept(fd, (struct sockaddr *)&addr, &addrlen);
  if (new_conn_fd == -1) {
    LOG_ERROR("Failed to accept");
  }

  // Dispatch by rand number
  int handler_id = next_handler_;

  // update next threadID
  next_handler_ = (next_handler_ + 1) % handlers_.size();

  std::shared_ptr<ConnectionHandlerTask> handler = handlers_[handler_id];
  LOG_DEBUG("Dispatching connection to worker %d", handler_id);

  handler->Notify(new_conn_fd);
}

void ConnectionDispatcherTask::ExitLoop() {
  NotifiableTask::ExitLoop();
  for (auto &handler : handlers_) handler->ExitLoop();
}

}  // namespace network
}  // namespace peloton
