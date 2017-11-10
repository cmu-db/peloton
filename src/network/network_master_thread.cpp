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


#include "network/network_master_thread.h"

#define MASTER_THREAD_ID -1

namespace peloton {
namespace network {
/*
 * Get the vector of Network worker threads
 */
std::vector<std::shared_ptr<NetworkWorkerThread>> &
NetworkMasterThread::GetWorkerThreads() {
  static std::vector<std::shared_ptr<NetworkWorkerThread>> worker_threads;
  return worker_threads;
}

/*
 * The Network master thread initialize num_threads worker threads on
 * constructor.
 */
NetworkMasterThread::NetworkMasterThread(const int num_threads,
                                           struct event_base *libevent_base)
    : NetworkThread(MASTER_THREAD_ID, libevent_base),
      num_threads_(num_threads),
      next_thread_id_(0) {
  auto &threads = GetWorkerThreads();
  threads.clear();
}

/*
 * Start the threads
 */
void NetworkMasterThread::Start() {
  auto &threads = GetWorkerThreads();

  // register thread to epoch manager.
  if (concurrency::EpochManagerFactory::GetEpochType() ==
      EpochType::DECENTRALIZED_EPOCH) {
    for (int thread_id = 0; thread_id < num_threads_; thread_id++) {
      concurrency::EpochManagerFactory::GetInstance().RegisterThread(thread_id);
    }
  }

  // create worker threads.
  for (int thread_id = 0; thread_id < num_threads_; thread_id++) {
    threads.push_back(std::shared_ptr<NetworkWorkerThread>(
        new NetworkWorkerThread(thread_id)));
    thread_pool.SubmitDedicatedTask(NetworkMasterThread::StartWorker,
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
void NetworkMasterThread::Stop() {
  auto &threads = GetWorkerThreads();

  for (int thread_id = 0; thread_id < num_threads_; thread_id++) {
    threads[thread_id].get()->SetThreadIsClosed(true);
  }

  // When a thread exit loop, the is_closed flag will be set to false
  // Wait for all threads exit loops
  for (int thread_id = 0; thread_id < num_threads_; thread_id++) {
    while (threads[thread_id].get()->GetThreadIsClosed()) {
      sleep(1);
    }
  }
}

/*
 * Start with worker event loop
 */
void NetworkMasterThread::StartWorker(NetworkWorkerThread *worker_thread) {
  event_base_loop(worker_thread->GetEventBase(), 0);
  // Set worker thread's close flag to false to indicate loop has exited
  worker_thread->SetThreadIsClosed(false);

  // Free events and event base
  if (worker_thread->GetThreadSockFd() != -1) {
    NetworkConnection *connection =
        NetworkManager::GetConnection(worker_thread->GetThreadSockFd());

    event_free(connection->network_event);
    event_free(connection->workpool_event);
    event_free(connection->logpool_event);
  }
  event_free(worker_thread->GetNewConnEvent());
  event_free(worker_thread->GetTimeoutEvent());
  event_base_free(worker_thread->GetEventBase());
}

/*
* Dispatch a new connection event to a random worker thread by
* writing to the worker's pipe
*/
void NetworkMasterThread::DispatchConnection(int new_conn_fd,
                                              short event_flags) {
  char buf[1];
  buf[0] = 'c';
  auto &threads = GetWorkerThreads();

  // Dispatch by rand number
  int thread_id = next_thread_id_;

  // update next threadID
  next_thread_id_ = (next_thread_id_ + 1) % num_threads_;

  std::shared_ptr<NetworkWorkerThread> worker_thread = threads[thread_id];
  LOG_DEBUG("Dispatching connection to worker %d", thread_id);

  std::shared_ptr<NewConnQueueItem> item(
      new NewConnQueueItem(new_conn_fd, event_flags, ConnState::CONN_READ));
  worker_thread->new_conn_queue.Enqueue(item);

  if (write(worker_thread->GetNewConnSendFd(), buf, 1) != 1) {
    LOG_ERROR("Failed to write to thread notify pipe");
  }
}

}  // namespace network
}  // namespace peloton
