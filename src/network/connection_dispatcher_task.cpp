//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_master_thread.cpp
//
// Identification: src/include/network/network_master_thread.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "network/connection_dispatcher_task.h"
#include "network/network_callback_util.h"

#define MASTER_THREAD_ID (-1)

namespace peloton {
namespace network {

/*
 * The Network master thread initialize num_threads worker threads on
 * constructor.
 */
ConnectionDispatcherTask::ConnectionDispatcherTask(int num_handlers)
    : NotifiableTask(MASTER_THREAD_ID),
      next_handler_(0) {

  RegisterSignalEvent(SIGHUP, CallbackUtil::OnSighup, this);


  // TODO(tianyu) Figure out what this initialization logic is doing and potentially rewrite
  // register thread to epoch manager.
  if (concurrency::EpochManagerFactory::GetEpochType() ==
      EpochType::DECENTRALIZED_EPOCH) {
    for (size_t task_id = 0; task_id < (size_t) num_handlers; task_id++) {
      concurrency::EpochManagerFactory::GetInstance().RegisterThread(task_id);
    }
  }

  // create worker threads.
  for (int task_id = 0; task_id < num_handlers; task_id++) {
    auto handler = std::make_shared<ConnectionHandlerTask>(task_id);
    handlers_.push_back(handler);
    thread_pool.SubmitDedicatedTask([=] {
      handler->EventLoop();
      // TODO(tianyu) WTF is this?
      // Set worker thread's close flag to false to indicate loop has exited
      handler->SetThreadIsClosed(false);
    });
  }

  // Wait for all threads ready to work
  for (int task_id = 0; task_id < num_handlers; task_id++) {
    while (!handlers_[task_id]->GetThreadIsStarted()) {
      sleep(1);
    }
  }
}


/*
 * Stop the threads
 */
void ConnectionDispatcherTask::Stop() {

  for (auto &handler : handlers_) {
    handler->SetThreadIsClosed(true);
  }

  // When a thread exit loop, the is_closed flag will be set to false
  // Wait for all threads exit loops
  for (auto &handler : handlers_) {
    while (handler->GetThreadIsClosed()) sleep(1);
  }
}


/*
* Dispatch a new connection event to a random worker thread by
* writing to the worker's pipe
*/
void ConnectionDispatcherTask::DispatchConnection(int fd) {
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof(addr);

  int new_conn_fd =
      accept(fd, (struct sockaddr *)&addr, &addrlen);
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

}  // namespace network
}  // namespace peloton
