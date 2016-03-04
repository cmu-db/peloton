//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_channel.h
//
// Identification: src/backend/message/rpc_channel.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "nanomsg.h"
#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include <string>

namespace peloton {
namespace message {

class RpcChannel : public google::protobuf::RpcChannel {
public:
  RpcChannel(const char* url);

  virtual ~RpcChannel();

  virtual void CallMethod(const google::protobuf::MethodDescriptor* method,
                          google::protobuf::RpcController* controller,
                          const google::protobuf::Message* request,
                          google::protobuf::Message* response,
                          google::protobuf::Closure* done);

  void Close();

  int GetSocket() {
      return socket_.GetSocket();
  }

private:
  NanoMsg socket_;
  int socket_id_;
};

}  // namespace message
}  // namespace peloton
