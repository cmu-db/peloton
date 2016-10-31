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

#include "wire/libevent_thread.h"
//#include "common/logger.h"
//#include "common/macros.h"
#include "common/init.h"
#include "common/thread_pool.h"
#include <sys/file.h>

namespace peloton {
namespace wire {

unsigned int LibeventThread::connection_thread_id = 0;
int LibeventThread::num_threads = 0;

std::vector<std::shared_ptr<LibeventThread>> &
LibeventThread::GetLibeventThreads() {
  static std::vector<std::shared_ptr<LibeventThread>> libevent_threads;
  return libevent_threads;
}

std::shared_ptr<LibeventThread> LibeventThread::GetLibeventThread(
    unsigned int conn_thread_id) {
  auto &threads = GetLibeventThreads();
  return threads[conn_thread_id];
}

void LibeventThread::Init(int num_threads) {
  auto &threads = GetLibeventThreads();
  for (int i = 0; i < num_threads; i++) {
    threads.push_back(std::shared_ptr<LibeventThread>(
        new LibeventThread(connection_thread_id++)));
    thread_pool.SubmitDedicatedTask(LibeventThread::Loop, threads[i].get());
  }
  LibeventThread::num_threads = num_threads;
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

void LibeventThread::Loop(peloton::wire::LibeventThread *libevent_thread) {
  event_base_loop(libevent_thread->libevent_base, 0);
}

LibeventThread::LibeventThread(unsigned int thread_id) : thread_id_(thread_id) {
  int fds[2];
  if (pipe(fds)) {
    LOG_ERROR("Can't create notify pipe to accept connections");
    exit(1);
  }

  new_conn_receive_fd_ = fds[0];
  new_conn_send_fd_ = fds[1];

  libevent_base = event_base_new();
  if (!libevent_base) {
    LOG_ERROR("Can't allocate event base\n");
    exit(1);
  }

  // Listen for notifications from other threads
  new_conn_event_ = event_new(libevent_base, new_conn_receive_fd_,
                              EV_READ | EV_PERSIST, ProcessConnection, this);

  if (event_add(new_conn_event_, 0) == -1) {
    LOG_ERROR("Can't monitor libevent notify pipe\n");
    exit(1);
  }

  // TODO init connection queue here
}
}
}
