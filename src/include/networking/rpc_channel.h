//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_channel.h
//
// Identification: src/backend/networking/rpc_channel.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "tcp_address.h"
#include "backend/common/logger.h"
#include "abstract_service.pb.h"

#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include <string>
#include <iostream>
#include <memory>

namespace peloton {
namespace networking {

class RpcChannel : public google::protobuf::RpcChannel {
 public:
  RpcChannel(const std::string& url);
  // RpcChannel(const long ip, const int port);

  virtual ~RpcChannel();

  virtual void CallMethod(const google::protobuf::MethodDescriptor* method,
                          google::protobuf::RpcController* controller,
                          const google::protobuf::Message* request,
                          google::protobuf::Message* response,
                          google::protobuf::Closure* done);

  void Close();

 private:
  NetworkAddress addr_;
};

}  // namespace networking
}  // namespace peloton
