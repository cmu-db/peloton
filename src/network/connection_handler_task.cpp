//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// connection_handler_task.cpp
//
// Identification: src/network/connection_handler_task.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/connection_handler_task.h"
#include "network/connection_handle.h"
#include "network/connection_handle_factory.h"

namespace peloton {
namespace network {

ConnectionHandlerTask::ConnectionHandlerTask(const int task_id)
    : NotifiableTask(task_id), new_conn_queue_(QUEUE_SIZE) {
  int fds[2];
  if (pipe(fds)) {
    LOG_ERROR("Can't create notify pipe to accept connections");
    exit(1);
  }
  new_conn_send_fd_ = fds[1];
  RegisterEvent(fds[0], EV_READ|EV_PERSIST,
                METHOD_AS_CALLBACK(ConnectionHandlerTask, HandleDispatch),
                this);
}

void ConnectionHandlerTask::Notify(int conn_fd) {
  new_conn_queue_.Enqueue(conn_fd);
  char buf[1];
  buf[0] = 'c';
  if (write(new_conn_send_fd_, buf, 1) != 1) {
    LOG_ERROR("Failed to write to thread notify pipe");
  }
}

void ConnectionHandlerTask::HandleDispatch(int new_conn_recv_fd, short) {
  // buffer used to receive messages from the main thread
  char m_buf[1];
  int client_fd;
  std::shared_ptr<ConnectionHandle> conn;

  // read the operation that needs to be performed
  if (read(new_conn_recv_fd, m_buf, 1) != 1) {
    LOG_ERROR("Can't read from the libevent pipe");
    return;
  }

  switch (m_buf[0]) {
    /* new connection case */
    case 'c': {
      // fetch the new connection fd from the queue
      new_conn_queue_.Dequeue(client_fd);
      conn = ConnectionHandleFactory::GetInstance().GetConnectionHandle(client_fd, this);
      break;
    }

    default:
    LOG_ERROR("Unexpected message. Shouldn't reach here");
  }
}

}  // namespace network
}  // namespace peloton