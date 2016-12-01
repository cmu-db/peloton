//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// libevent_thread.cpp
//
// Identification: src/wire/libevent_thread.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "wire/libevent_thread.h"
#include <sys/file.h>
#include <fstream>
#include <vector>
#include "boost/thread/future.hpp"
#include "common/init.h"
#include "common/thread_pool.h"
#include "wire/libevent_server.h"

namespace peloton {
namespace wire {

/*
 * Get the vector of libevent worker threads
 */
std::vector<std::shared_ptr<LibeventWorkerThread>>
    &LibeventMasterThread::GetWorkerThreads() {
  static std::vector<std::shared_ptr<LibeventWorkerThread>> worker_threads;
  return worker_threads;
}

/*
 * The libevent master thread initialize num_threads worker threads on
 * constructor.
 */
LibeventMasterThread::LibeventMasterThread(const int num_threads,
                                           struct event_base *libevent_base)
    : LibeventThread(MASTER_THREAD_ID, libevent_base),
      num_threads_(num_threads) {
  auto &threads = GetWorkerThreads();
  for (int thread_id = 0; thread_id < num_threads; thread_id++) {
    threads.push_back(std::shared_ptr<LibeventWorkerThread>(
        new LibeventWorkerThread(thread_id)));
    thread_pool.SubmitDedicatedTask(LibeventMasterThread::StartWorker,
                                    threads[thread_id].get());
  }
  // TODO wait for all threads to be up before exit from Init()
  // TODO replace sleep with future/promises
  sleep(1);
}

/*
 * Start with worker event loop
 */
void LibeventMasterThread::StartWorker(LibeventWorkerThread *worker_thread) {
  event_base_loop(worker_thread->GetEventBase(), 0);
}

/*
* The worker thread creates a pipe for master-worker communication on
* constructor.
*/
LibeventWorkerThread::LibeventWorkerThread(const int thread_id)
    : LibeventThread(thread_id, event_base_new()), new_conn_queue(QUEUE_SIZE) {
  int fds[2];
  if (pipe(fds)) {
    LOG_ERROR("Can't create notify pipe to accept connections");
    exit(1);
  }
  // send_fd is used by the master thread, received_fd used by worker thread
  new_conn_receive_fd = fds[0];
  new_conn_send_fd = fds[1];

  // Listen for notifications from the master thread
  new_conn_event_ = event_new(libevent_base_, new_conn_receive_fd,
                              EV_READ | EV_PERSIST, WorkerHandleNewConn, this);

  if (event_add(new_conn_event_, 0) == -1) {
    LOG_ERROR("Can't monitor libevent notify pipe\n");
    exit(1);
  }
}

/*
* Dispatch a new connection event to a random worker thread by
* writing to the worker's pipe
*/
void LibeventMasterThread::DispatchConnection(int new_conn_fd,
                                              short event_flags) {
  char buf[1];
  buf[0] = 'c';
  auto &threads = GetWorkerThreads();

  // Dispatch by rand number
  int thread_id = next_thread_id_;

  // update next threadID
  next_thread_id_ = (next_thread_id_ + 1) % num_threads_;

  std::shared_ptr<LibeventWorkerThread> worker_thread = threads[thread_id];
  LOG_DEBUG("Dispatching connection to worker %d", thread_id);

  std::shared_ptr<NewConnQueueItem> item(
      new NewConnQueueItem(new_conn_fd, event_flags, CONN_READ));
  worker_thread->new_conn_queue.Enqueue(item);

  if (write(worker_thread->new_conn_send_fd, buf, 1) != 1) {
    LOG_ERROR("Failed to write to thread notify pipe");
  }
}
}
}
