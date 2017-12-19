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
 * Get the vector of Network worker threads
 */
std::vector<std::shared_ptr<ConnectionHandlerTask>> &
ConnectionDispatcherTask::GetHandlers() {
  static std::vector<std::shared_ptr<ConnectionHandlerTask>> handlers;
  return handlers;
}

/*
 * The Network master thread initialize num_threads worker threads on
 * constructor.
 */
ConnectionDispatcherTask::ConnectionDispatcherTask(int num_threads)
    : NotifiableTask(MASTER_THREAD_ID),
      num_threads_(num_threads),
      next_handler(0) {
  // TODO(tianyu): huh?
  auto &handlers = GetHandlers();
  handlers.clear();

  RegisterSignalEvent(SIGHUP, CallbackUtil::OnSighup, this);
}

/*
 * Start the threads
 */
// TODO(tianyu) Rename and write better
void ConnectionDispatcherTask::Start() {
  auto &threads = GetHandlers();

  // register thread to epoch manager.
  if (concurrency::EpochManagerFactory::GetEpochType() ==
      EpochType::DECENTRALIZED_EPOCH) {
    for (int thread_id = 0; thread_id < num_threads_; thread_id++) {
      concurrency::EpochManagerFactory::GetInstance().RegisterThread(thread_id);
    }
  }

  // create worker threads.
  for (int thread_id = 0; thread_id < num_threads_; thread_id++) {
    threads.push_back(std::make_shared<ConnectionHandlerTask>(thread_id));
    thread_pool.SubmitDedicatedTask(ConnectionDispatcherTask::StartHandler,
                                    threads[thread_id].get());
  }

  // Wait for all threads ready to work
  for (int thread_id = 0; thread_id < num_threads_; thread_id++) {
    while (!threads[thread_id].get()->GetThreadIsStarted()) {
      sleep(1);
    }
  }
}

/*
 * Stop the threads
 */
void ConnectionDispatcherTask::Stop() {
  auto &handlers = GetHandlers();

  for (int handler_id = 0; handler_id < num_threads_; handler_id++) {
    handlers[handler_id].get()->SetThreadIsClosed(true);
  }

  // When a thread exit loop, the is_closed flag will be set to false
  // Wait for all threads exit loops
  for (int handler_id = 0; handler_id < num_threads_; handler_id++) {
    while (handlers[handler_id].get()->GetThreadIsClosed()) {
      sleep(1);
    }
  }
}

/*
 * Start with worker event loop
 */
void ConnectionDispatcherTask::StartHandler(ConnectionHandlerTask *handler) {
  handler->EventLoop();
  // TODO(tianyu): Is it just me or does this flag means the fucking opposite of what it says?
  // Set worker thread's close flag to false to indicate loop has exited
  handler->SetThreadIsClosed(false);
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

  auto &handlers = GetHandlers();

  // Dispatch by rand number
  int handler_id = next_handler;

  // update next threadID
  next_handler = (next_handler + 1) % num_threads_;

  std::shared_ptr<ConnectionHandlerTask> handler = handlers[handler_id];
  LOG_DEBUG("Dispatching connection to worker %d", handler_id);

  handler->Notify(fd);
}

}  // namespace network
}  // namespace peloton
