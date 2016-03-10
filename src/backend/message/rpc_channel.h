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
#include "tcp_address.h"
#include "backend/common/logger.h"
#include "abstract_service.pb.h"

#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include <string>
#include <iostream>
#include <memory>

namespace peloton {
namespace message {

class RpcChannel : public google::protobuf::RpcChannel {
public:
  RpcChannel(const std::string& url);
  //RpcChannel(const long ip, const int port);

  virtual ~RpcChannel();

  virtual void CallMethod(const google::protobuf::MethodDescriptor* method,
                          google::protobuf::RpcController* controller,
                          const google::protobuf::Message* request,
                          google::protobuf::Message* response,
                          google::protobuf::Closure* done);

  void Close();

//  int GetSocket() {
//      return socket_.GetSocket();
//  }

    static void Callback(std::shared_ptr<NanoMsg> psocket) {

        LOG_TRACE("This is client test callback: socket");
        int bytes = 0;

        while (bytes <= 0) {

            // Receive message
            char* buf = NULL;
            // wait to receive the response
            bytes = psocket->Receive(&buf, NN_MSG, 0);
            if (bytes <= 0) {
                LOG_TRACE("receive nothing and continue");
                continue;
            }

            HeartbeatResponse response;
            // deserialize the receiving msg
            response.ParseFromString(buf);

            if (response.has_sender_site() == true) {
                std::cout << "sender site: " << response.sender_site()
                        << std::endl;
            } else {
                std::cout << "No response: sender site" << std::endl;
            }

            if (response.has_status() == true) {
                std::cout << "Status: " << response.status() << std::endl;
            } else {
                std::cout << "No response: sender status" << std::endl;
            }

            // free the receiving buf
            freemsg(buf);

        }

    }

private:
//  std::shared_ptr<NanoMsg> psocket_;
//  int socket_id_;
    NetworkAddress addr_;
};

}  // namespace message
}  // namespace peloton
