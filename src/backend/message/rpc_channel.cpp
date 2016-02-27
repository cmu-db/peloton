//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_message.h
//
// Identification: src/backend/message/abstract_message.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "rpc_channel.h"

#include <google/protobuf/descriptor.h>
#include <city.h>
#include <iostream>

namespace peloton {
namespace message {

RpcChannel::RpcChannel(const char* url) :
  socket_(AF_SP, NN_REQ),
  socket_id_(socket_.Connect(url))
{
}

RpcChannel::~RpcChannel()
{
  Close();
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                                  google::protobuf::RpcController* controller,
                                  const google::protobuf::Message* request,
                                  google::protobuf::Message* response,
                                  google::protobuf::Closure* done)
{
  // Get the rpc function name
  std::string methodname = std::string( method->full_name() );

  // Get the hashcode for the rpc function name
  uint64_t opcode = CityHash64(methodname.c_str(), methodname.length());

  // prepare the sending buf
  size_t msg_len = request->ByteSize() + sizeof(opcode);
  char* buf = (char*)allocmsg(msg_len, 0);

  // copy the hashcode into the buf
  memcpy(buf, &opcode, sizeof(opcode));

  // call protobuf to serialize the request message into sending buf
  request->SerializeToArray(buf + sizeof(opcode), request->ByteSize());

  // send the message to server
  socket_.Send(buf,msg_len,0);

  // call nanomsg function to free the buf
  freemsg(buf);

  // wait to receive the response
  socket_.Receive(&buf, NN_MSG, 0);

  // deserialize the receiving msg
  response->ParseFromString(buf);

  // free the receiving buf
  freemsg(buf);

  // run call back function
  if (done != NULL) {
    done->Run();
  }

  //TODO: Use controller there
  std::cout << controller << std::endl;
}

void RpcChannel::Close()
{
  if (socket_id_ > 0) {
    socket_.Shutdown(socket_id_);
    socket_id_ = 0;
  }
}


}  // namespace message
}  // namespace peloton
