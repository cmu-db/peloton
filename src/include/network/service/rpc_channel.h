//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_channel.h
//
// Identification: src/include/network/service/rpc_channel.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <memory>

#include "network_address.h"
#include "common/logger.h"
#include "peloton/proto/abstract_service.pb.h"

#include <google/protobuf/service.h>
#include <google/protobuf/message.h>

namespace peloton {
namespace network {
namespace service {

class RpcChannel : public google::protobuf::RpcChannel {
 public:
  RpcChannel(const std::string &url);
  // RpcChannel(const long ip, const int port);

  virtual ~RpcChannel();

  virtual void CallMethod(const google::protobuf::MethodDescriptor *method,
                          google::protobuf::RpcController *controller,
                          const google::protobuf::Message *request,
                          google::protobuf::Message *response,
                          google::protobuf::Closure *done);

  void Close();

 private:
  NetworkAddress addr_;
};

}  // namespace service
}  // namespace network
}  // namespace peloton
