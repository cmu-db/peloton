//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_controller.h
//
// Identification: src/backend/message/rpc_controller.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include <google/protobuf/service.h>

namespace peloton {
namespace networking {

class RpcController : public google::protobuf::RpcController {
	      std::string _error_str;
  bool _is_failed;
  public:
    RpcController() { Reset(); }
    void Reset() {
      _error_str = "";
      _is_failed = false;
    }

    bool Failed() const {
      return _is_failed;
    }

    std::string ErrorText() const {
      return _error_str;
    }

    void StartCancel() { // NOT IMPL
      return ;
    }

    void SetFailed(const std::string &reason) {

      _is_failed = true;
      _error_str = reason;
    }

    bool IsCanceled() const { // NOT IMPL
      return false;
    }
  		
    void NotifyOnCancel(google::protobuf::Closure* callback) { // NOT IMPL
        return;
    }
};

}  // namespace networking
}  // namespace peloton
