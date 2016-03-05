//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_client_manager.h
//
// Identification: /peloton/src/backend/message/rpc_client_manager.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "nanomsg.h"
#include "rpc_client.h"

#include <map>
#include <mutex>
#include <memory>
#include <functional>

namespace peloton {
namespace message {

class RpcClientManager {

public:
public:
    // global singleton
    static RpcClientManager &GetInstance(void);

    void SetCallback(std::shared_ptr<NanoMsg> socket, std::function<void()> callback);

    void DeleteCallback(int key);

private:

    RpcClientManager();
    ~RpcClientManager();

    void FdSet(int socket);
    void FdLoop();

    // poll_fds is a mutex for FdLoop thread and FdSet function
    pollfd* poll_fds_;
    int poll_fds_count_;
    std::mutex poll_fds_mutex_;

    std::map<int, std::function<void()>> sock_func_;
    std::mutex sock_func_mutex;
};

}  // namespace message
}  // namespace peloton
