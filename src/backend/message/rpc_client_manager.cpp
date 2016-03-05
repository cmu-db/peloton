//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_client_manager.cpp
//
// Identification: /peloton/src/backend/message/rpc_client_manager.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "rpc_client_manager.h"
#include "backend/common/thread_manager.h"

namespace peloton {
namespace message {

// global singleton
RpcClientManager &RpcClientManager::GetInstance(void) {
  static RpcClientManager client_manager;
  return client_manager;
}

RpcClientManager::RpcClientManager() :
        poll_fds_ (nullptr),
        poll_fds_count_ (0) {
}

RpcClientManager::~RpcClientManager() {

    if ( poll_fds_ != nullptr) {
        free (poll_fds_);
    }
}

void RpcClientManager::SetCallback(std::shared_ptr<NanoMsg> socket, std::function<void()> callback) {

    int sock = socket->GetSocket();

    // the callback thread will delete this item
    std::lock_guard<std::mutex> guard(sock_func_mutex);
    sock_func_.insert(std::make_pair(sock, callback));
}

void RpcClientManager::DeleteCallback(int key) {

    // the callback thread will delete this item
    std::lock_guard<std::mutex> guard(sock_func_mutex);

    auto iter = sock_func_.find(key);
    if (iter != sock_func_.end()) {
        sock_func_.erase(iter);
    }
}

void RpcClientManager::FdSet(int socket) {

    // scope lock
    std::lock_guard<std::mutex> guard(poll_fds_mutex_);

    pollfd* more_poll_fds = (pollfd*) realloc( poll_fds_, sizeof(pollfd) );

    if (more_poll_fds != NULL) {
        poll_fds_ = more_poll_fds;
        poll_fds_count_ ++;
        poll_fds_[poll_fds_count_-1].fd = socket;
        poll_fds_[poll_fds_count_-1].events = NN_POLLIN;
    } else {
        LOG_TRACE ("Error (re)allocating memory");
    }
}

void RpcClientManager::FdLoop(){

    // Loop listening the socket_inproc_
    while (true) {

        // scope lock
        std::lock_guard<std::mutex> guard(poll_fds_mutex_);
        int rc = poll (poll_fds_, poll_fds_count_, 0);

        if (rc == 0) {
            LOG_TRACE ("Timeout when check fd");
            continue;
        }
        if (rc == -1) {
            LOG_TRACE ("Error when check fd");
            continue;
        }

        // check out every socket
        for (int it = 0; it < poll_fds_count_; it++) {

            // if Message can be received, put recv task in thread pool
            if (poll_fds_[it].revents & NN_POLLIN) {
                LOG_TRACE ("Message can be received from fd: %d", poll_fds_[it].fd);

                int socket = poll_fds_[it].fd;
                auto iter = sock_func_.find(socket);

                // prepaere workers
                std::function<void()> callback = iter->second;

                // add workers to thread pool
                ThreadManager::GetInstance().AddTask(callback);
            }
        } // end for
    } // end while
}

}  // namespace message
}  // namespace peloton

