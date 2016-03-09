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

#include <google/protobuf/descriptor.h>
#include <iostream>
#include <functional>

namespace peloton {
namespace networking {

RpcChannel::RpcChannel(const char* url) :
      socket_id_(socket_.Connect(url)){
}

RpcChannel::~RpcChannel(){
  Close();
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done){
  // Get the rpc function name
  std::string methodname = std::string( method->full_name() );
  std::hash<std::string> string_hash_fn;

  // Get the hashcode for the rpc function name
  // TODO:
  //uint64_t opcode = CityHash64(methodname.c_str(), methodname.length());
  size_t opcode = string_hash_fn(methodname);

  // prepare the sending buf
  size_t msg_len = request->ByteSize() + sizeof(opcode);
  char* buf = new char[msg_len];

  // copy the hashcode into the buf
  memcpy(buf, &opcode, sizeof(opcode));

  // call protobuf to serialize the request message into sending buf
  request->SerializeToArray(buf + sizeof(opcode), request->ByteSize());

  // send the message to server
  socket_.Send(buf,msg_len,0);

  // call nanomsg function to free the buf
  delete buf;

  // wait to receive the response
  socket_.Receive(&buf, 0, 0);

  // deserialize the receiving msg
  response->ParseFromString(buf);

  // free the receiving buf
  delete buf;

  // run call back function
  if (done != NULL) {
    done->Run();
  }

  //TODO: Use controller there
  std::cout << controller << std::endl;
}

void RpcChannel::Close(){
  if (socket_id_ > 0) {
    socket_.Shutdown(socket_id_);
    socket_id_ = 0;
  }
}


}  // namespace networking
}  // namespace peloton
