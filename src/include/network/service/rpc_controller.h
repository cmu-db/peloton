//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_controller.h
//
// Identification: src/include/network/service/rpc_controller.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <google/protobuf/service.h>
#include <string>

namespace peloton {
namespace network {
namespace service {

class RpcController : public google::protobuf::RpcController {
 public:
  RpcController() { Reset(); }

  void Reset() {
    error_str_ = "";
    is_failed_ = false;
  }

  // client side
  bool Failed() const { return is_failed_; }

  // client side
  std::string ErrorText() const { return error_str_; }

  // client side
  void StartCancel() {  // NOT IMPL
    return;
  }

  // sever side
  void SetFailed(const std::string &reason) {
    is_failed_ = true;
    error_str_ = reason;
  }

  // sever side
  bool IsCanceled() const {  // NOT IMPL
    return false;
  }

  // sever side
  void NotifyOnCancel(google::protobuf::Closure *callback) {  // NOT IMPL

    if (callback) {
      callback->Run();
    }

    return;
  }

 private:
  std::string error_str_;
  bool is_failed_;
};

}  // namespace service
}  // namespace network
}  // namespace peloton
