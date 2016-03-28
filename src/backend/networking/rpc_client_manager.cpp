//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_client_manager.cpp
//
// Identification: src/backend/networking/rpc_client_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "rpc_client_manager.h"
#include "backend/common/logger.h"
#include "backend/common/thread_manager.h"
#include "backend/common/thread_manager.h"

#include <unistd.h>

namespace peloton {
namespace networking {

// global singleton
RpcClientManager& RpcClientManager::GetInstance(void) {
  static RpcClientManager client_manager;
  return client_manager;
}

RpcClientManager::RpcClientManager() : poll_fds_(NULL), poll_fds_count_(0) {
  LOG_TRACE("This is RpcClientManager Constructor");

  // prepaere workers
  std::function<void()> listener = std::bind(&RpcClientManager::FdLoop, this);

  // add workers to thread pool
  ThreadManager::GetInstance().AddTask(listener);
}

RpcClientManager::~RpcClientManager() {
  if (poll_fds_ != nullptr) {
    free(poll_fds_);
  }
}

void RpcClientManager::DeleteCallback(int key) {
  // the callback thread will delete this item
  std::unique_lock<std::mutex> lock(poll_fds_mutex_);

  auto iter = sock_func_.find(key);
  if (iter != sock_func_.end()) {
    sock_func_.erase(iter);
  }

  lock.unlock();
}

void RpcClientManager::FdSet(int socket) {
  pollfd* more_poll_fds =
      (pollfd*)realloc(poll_fds_, sizeof(pollfd) * (poll_fds_count_ + 1));

  if (more_poll_fds != NULL) {
    poll_fds_ = more_poll_fds;
    poll_fds_count_++;
    poll_fds_[poll_fds_count_ - 1].fd = socket;
    poll_fds_[poll_fds_count_ - 1].events = NN_POLLIN;

    // check out every socket
    for (int it = 0; it < poll_fds_count_; it++) {
      // if Message can be received, put recv task in thread pool
      LOG_TRACE("Client check fd%d: %d", it, poll_fds_[it].fd);

    }  // end for
  } else {
    LOG_TRACE("Error (re)allocating memory");
  }
}

void RpcClientManager::FdLoop() {
  // Loop listening the socket_inproc_
  while (true) {
    std::unique_lock<std::mutex> lock(poll_fds_mutex_);

    LOG_TRACE("FdLoop: Get lock");

    cond_.wait(lock);

    int rc = poll(poll_fds_, poll_fds_count_, 100);

    if (rc == 0) {
      LOG_TRACE("Client: Timeout when check fd in RpcClientManager");
      lock.unlock();
      continue;
    }
    if (rc == -1) {
      LOG_TRACE("Error when check fd in RpcClientManager");
      lock.unlock();
      continue;
    }

    // check out every socket
    for (int it = 0; it < poll_fds_count_; it++) {
      // if Message can be received, put recv task in thread pool
      if (poll_fds_[it].revents & NN_POLLIN) {
        LOG_TRACE("Client: Message can be received from fd: %d",
                  poll_fds_[it].fd);

        int socket = poll_fds_[it].fd;
        auto iter = sock_func_.find(socket);

        // prepaere workers
        std::function<void()> callback = iter->second;

        // add workers to thread pool
        ThreadManager::GetInstance().AddTask(callback);
      }
    }  // end for

    lock.unlock();
  }  // end while
}

}  // namespace networking
}  // namespace peloton
