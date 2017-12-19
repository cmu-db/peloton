//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_worker_thread.cpp
//
// Identification: src/network/network_worker_thread.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/connection_handler_task.h"
#include "network/network_callback_util.h"

namespace peloton {
namespace network {

/*
* The worker thread creates a pipe for master-worker communication on
* constructor.
*/
ConnectionHandlerTask::ConnectionHandlerTask(const int task_id)
    : NotifiableTask(task_id), new_conn_queue(QUEUE_SIZE) {
  int fds[2];
  if (pipe(fds)) {
    LOG_ERROR("Can't create notify pipe to accept connections");
    exit(1);
  }
  new_conn_receive_fd = fds[0];
  new_conn_send_fd_ = fds[1];
  RegisterEvent(fds[0], EV_READ|EV_PERSIST, CallbackUtil::OnNewConnectionDispatch, this);
  struct timeval one_second = {1, 0};
  RegisterPeriodicEvent(&one_second, CallbackUtil::ThreadControl_Callback, this);
}

void ConnectionHandlerTask::Notify(int conn_fd) {
  std::shared_ptr<NewConnQueueItem> item =
      std::make_shared<NewConnQueueItem>(conn_fd, EV_READ|EV_PERSIST, ConnState::CONN_READ);
  new_conn_queue.Enqueue(item);
  char buf[1];
  buf[0] = 'c';
  if (write(new_conn_send_fd_, buf, 1) != 1) {
    LOG_ERROR("Failed to write to thread notify pipe");
  }
}

}  // namespace network
}  // namespace peloton