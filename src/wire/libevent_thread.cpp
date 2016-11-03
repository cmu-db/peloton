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
#include <fstream>
#include <vector>

#include "wire/libevent_server.h"
#include "common/init.h"
#include "common/thread_pool.h"
#include <sys/file.h>

namespace peloton {
namespace wire {

std::vector<std::shared_ptr<LibeventWorkerThread>> &
LibeventMasterThread::GetWorkerThreads() {
  static std::vector<std::shared_ptr<LibeventWorkerThread>> worker_threads;
  return worker_threads;
}

LibeventMasterThread::LibeventMasterThread(const int num_threads,
                                           struct event_base *libevent_base)
    : LibeventThread(MASTER_THREAD_ID, libevent_base),
      num_threads_(num_threads) {
  auto &threads = GetWorkerThreads();
  for (int i = 0; i < num_threads; i++) {
    threads.push_back(
        std::shared_ptr<LibeventWorkerThread>(new LibeventWorkerThread(i)));
    thread_pool.SubmitDedicatedTask(LibeventMasterThread::StartWorker,
                                    threads[i].get());
  }
  // TODO wait for all threads to be up before exit from Init()
}

void LibeventThread::EventHandler(evutil_socket_t fd, short event, void *arg) {
  // FIXME arg should be a connection ptr
  (void)arg;
  (void)event;
  (void)fd;

  // LOG_ERROR("Handler invoked");
  bool done = false;
  int state = 1;
  // Assume 1 is for LISTEN_STATE..
  // TODO Define all different states as readable variables in header

  while (done == false) {
    switch (state) {
      case 1: {

        struct sockaddr_storage addr;
        socklen_t addrlen = sizeof(addr);
        int client_fd = accept(fd, (struct sockaddr *)&addr, &addrlen);
        if (client_fd == -1) {
          LOG_ERROR("Failed to accept");
        }
        DispatchConnection();
        done = true;
        break;
      }
      // TODO handle other states, too
      default: {}
    }
  }
}

void LibeventThread::CreateConnection(int client_fd, struct event_base *base) {

  // TODO create connection object for the connection

  struct ConnectionPlaceHolder *conn = new ConnectionPlaceHolder();

  // TODO pass the main thread connection object here, instead of `base`
  conn->event = event_new(base, client_fd, EV_READ | EV_PERSIST,
                          LibeventThread::EventHandler, conn);
  if (event_add(conn->event, 0) == -1) {
    LOG_ERROR("Event add failed\n");
    exit(1);
  }
}

void LibeventThread::DispatchConnection() {

  char buf[1];
  buf[0] = 'c';

  // Dispatch by rand number
  std::shared_ptr<LibeventThread> libev_thread =
      LibeventThread::GetLibeventThread(rand() % LibeventThread::num_threads);

  // TODO enqueue an item in the queue
  // queue->enqueue(sth);

  // TODO get the correct libevent thread object
  if (write(libev_thread->new_conn_send_fd_, buf, 1) != 1) {
    LOG_ERROR("Writing to thread notify pipe");
  }
}

void LibeventThread::ProcessConnection(evutil_socket_t fd, short event,
                                       void *arg) {

  // FIXME We should pass the connection as arg instead of thread.
  LibeventThread *libevent_thread = (LibeventThread *)arg;
  (void)fd;
  (void)event;

  char buf[1];

  if (read(fd, buf, 1) != 1) {
    LOG_ERROR("Can't read from libevent pipe\n");
    return;
  }

  // TODO Pull pending connection requests from the queue
  // queue->deque

  LOG_ERROR("Thread %d is processing conn request",
            (int)libevent_thread->thread_id_);

  CreateConnection(fd, libevent_thread->libevent_base);
  //
}

void LibeventMasterThread::StartWorker(LibeventWorkerThread *worker_thread) {
  event_base_loop(worker_thread->GetEventBase(), 0);
}

LibeventWorkerThread::LibeventWorkerThread(const int thread_id)
    : LibeventThread(thread_id, event_base_new()), new_conn_queue(QUEUE_SIZE) {
  int fds[2];
  if (pipe(fds)) {
    LOG_ERROR("Can't create notify pipe to accept connections");
    exit(1);
  }

  new_conn_receive_fd = fds[0];
  new_conn_send_fd = fds[1];

  // Listen for notifications from other threads
  new_conn_event_ = event_new(libevent_base_, new_conn_receive_fd,
                              EV_READ | EV_PERSIST, WorkerHandleNewConn, this);

  if (event_add(new_conn_event_, 0) == -1) {
    LOG_ERROR("Can't monitor libevent notify pipe\n");
    exit(1);
  }

  // TODO init connection queue here
}

void LibeventMasterThread::DispatchConnection(int new_conn_fd,
                                              short event_flags) {
  char buf[1];
  buf[0] = 'c';

  auto &threads = GetWorkerThreads();

  // Dispatch by rand number
  std::shared_ptr<LibeventWorkerThread> worker_thread =
      threads[rand() % num_threads_];

  // TODO: Add init_state arg
  std::shared_ptr<NewConnQueueItem> item(
      new NewConnQueueItem(new_conn_fd, event_flags, CONN_READ));
  worker_thread->new_conn_queue.Enqueue(item);

  if (write(worker_thread->new_conn_send_fd, buf, 1) != 1) {
    LOG_ERROR("Failed to write to thread notify pipe");
  }
}
}
}
