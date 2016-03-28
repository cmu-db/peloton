//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_client_manager.h
//
// Identification: src/backend/networking/rpc_client_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <mutex>
#include <memory>
#include <functional>
#include <condition_variable>

namespace peloton {
namespace networking {

class RpcClientManager {
 public:
 public:
  // global singleton
  static RpcClientManager& GetInstance(void);

  // void SetCallback(std::shared_ptr<NanoMsg> socket, std::function<void()>
  // callback);

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
  std::condition_variable cond_;

  std::map<int, std::function<void()>> sock_func_;
};

}  // namespace networking
}  // namespace peloton
