//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_channel.h
//
// Identification: src/backend/message/rpc_channel.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "rpc_channel.h"
#include "rpc_controller.h"
#include "rpc_client_manager.h"
#include "nanomsg.h"
#include "backend/common/logger.h"

#include <google/protobuf/descriptor.h>
#include <iostream>
#include <functional>

namespace peloton {
namespace message {

//RpcChannel::RpcChannel(const char* url) :
//  socket_(AF_SP, NN_REQ),
//  socket_id_(socket_.Connect(url)) {
//}

RpcChannel::RpcChannel(const char* url) //:
  //psocket_(std::make_shared<NanoMsg>(AF_SP, NN_REQ)),
//  psocket_(new NanoMsg(AF_SP, NN_REQ)),
//  socket_id_(psocket_->Connect(url))
{
          psocket_ = std::make_shared<NanoMsg>(AF_SP, NN_REQ);
          socket_id_ = psocket_->Connect(url);
}

RpcChannel::~RpcChannel() {
  Close();
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                                  google::protobuf::RpcController* controller,
                                  const google::protobuf::Message* request,
                                  google::protobuf::Message* response,
                                  google::protobuf::Closure* done) {

  if (controller->Failed()) {
      std::string error = controller->ErrorText();
      LOG_TRACE( "RpcChannel with controller failed:%s ", error.c_str() );
  }

  // Get the rpc function name
  std::string methodname = std::string( method->full_name() );
  std::hash<std::string> string_hash_fn;

  // Get the hashcode for the rpc function name
  // TODO:
  //uint64_t opcode = CityHash64(methodname.c_str(), methodname.length());
  size_t opcode = string_hash_fn(methodname);

  // prepare the sending buf
  size_t msg_len = request->ByteSize() + sizeof(opcode);
  char* buf = (char*)allocmsg(msg_len, 0);

  // copy the hashcode into the buf
  memcpy(buf, &opcode, sizeof(opcode));

  // call protobuf to serialize the request message into sending buf
  request->SerializeToArray(buf + sizeof(opcode), request->ByteSize());

  // send the message to server
  psocket_->Send(buf,msg_len,0);

  // call nanomsg function to free the buf
  freemsg(buf);

  std::function<void()> call = std::bind(Callback, psocket_);
  RpcClientManager::GetInstance().SetCallback(this->psocket_, call);

//  // wait to receive the response
//  psocket_->Receive(&buf, NN_MSG, 0);
//
//  // deserialize the receiving msg
//  response->ParseFromString(buf);
//
//  // free the receiving buf
//  freemsg(buf);

  // run call back function
  if (done != NULL) {
    done->Run();
  }

  if (response != NULL) {
      std::cout << response << std::endl;
  }
}

void RpcChannel::Close() {

  if (socket_id_ > 0) {
    psocket_->Shutdown(socket_id_);
    socket_id_ = 0;
  }
}


}  // namespace message
}  // namespace peloton
