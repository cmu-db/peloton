//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_handler_task.cpp
//
// Identification: src/network/connection_handler_task.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/connection_handler_task.h"
#include "network/connection_handle.h"
#include "network/connection_handle_factory.h"

namespace peloton {
namespace network {

ConnectionHandlerTask::ConnectionHandlerTask(const int task_id)
    : NotifiableTask(task_id) {
  int fds[2];
  if (pipe(fds)) {
    LOG_ERROR("Can't create notify pipe to accept connections");
    exit(1);
  }
  new_conn_send_fd_ = fds[1];
  RegisterEvent(fds[0], EV_READ | EV_PERSIST,
                METHOD_AS_CALLBACK(ConnectionHandlerTask, HandleDispatch),
                this);
}

void ConnectionHandlerTask::Notify(int conn_fd) {
  int buf[1];
  buf[0] = conn_fd;
  if (write(new_conn_send_fd_, buf, sizeof(int)) != sizeof(int)) {
    LOG_ERROR("Failed to write to thread notify pipe");
  }
}

void ConnectionHandlerTask::HandleDispatch(int new_conn_recv_fd, short) {
  // buffer used to receive messages from the main thread
  char client_fd[sizeof(int)];
  size_t bytes_read = 0;

  // read fully
  while (bytes_read < sizeof(int)) {
    ssize_t result = read(new_conn_recv_fd, client_fd + bytes_read,
                          sizeof(int) - bytes_read);
    if (result < 0) {
      LOG_ERROR("Error when reading from dispatch");
    }
    bytes_read += (size_t)result;
  }
  ConnectionHandleFactory::GetInstance()
      .NewConnectionHandle(*reinterpret_cast<int *>(client_fd), this)
      .RegisterToReceiveEvents();
}

}  // namespace network
}  // namespace peloton