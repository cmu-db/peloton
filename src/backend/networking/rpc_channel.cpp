//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_channel.h
//
// Identification: src/backend/networking/rpc_channel.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/networking/rpc_type.h"
#include "backend/networking/rpc_client.h"
#include "backend/networking/rpc_channel.h"
#include "backend/networking/rpc_controller.h"
#include "backend/networking/tcp_connection.h"
#include "backend/common/thread_manager.h"
#include "backend/common/logger.h"

#include <google/protobuf/descriptor.h>

#include <iostream>
#include <functional>

namespace peloton {
namespace networking {

RpcChannel::RpcChannel(const std::string& url) :
        addr_(url) {
}

RpcChannel::~RpcChannel() {
  Close();
}

////////////////////////////////////////////////////////////////////////////////
//                 Request message structure:
// --Header:  message length (Opcode + request),         uint32_t (4bytes)
// --Opcode:  std::hash(methodname)-->Opcode,            uint64_t (8bytes)
// --Request: the serialization result of protobuf       Header-8
//
// TODO: We did not add checksum code in this version
////////////////////////////////////////////////////////////////////////////////

/*
 * Channel is only use by protobuf rpc client. So the this method is sending request msg
 */
void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            __attribute__((unused)) google::protobuf::Message* response,
                            google::protobuf::Closure* done) {
  assert(response != nullptr);

  if (controller->Failed()) {
      std::string error = controller->ErrorText();
      LOG_TRACE( "RpcChannel with controller failed:%s ", error.c_str() );
  }

  // run call back function
  if (done != NULL) {
     done->Run();
  }

  // Get the rpc function name
  std::string methodname = std::string( method->full_name() );
  std::hash<std::string> string_hash_fn;

  // Get the hashcode for the rpc function name
  // we use unit64_t because we should specify the exact length
  uint64_t opcode = string_hash_fn(methodname);

  // prepare the sending buf
  uint32_t msg_len = request->ByteSize() + sizeof(opcode);

  // total length of the message: header length (4bytes) + message length (8bytes + ...)
  assert(HEADERLEN == sizeof(msg_len));
  char buf[sizeof(msg_len) + msg_len];

  // copy the header into the buf
  memcpy(buf, &msg_len, sizeof(msg_len));

  // copy the hashcode into the buf, following the header
  assert(OPCODELEN == sizeof(opcode));
  memcpy(buf + sizeof(msg_len), &opcode, sizeof(opcode));

  // call protobuf to serialize the request message into sending buf
  request->SerializeToArray(buf + sizeof(msg_len) + sizeof(opcode), request->ByteSize());

  // crate a connection to prcess the rpc send and recv
  std::shared_ptr<Connection> conn(new Connection(-1, NULL));

  // Client connection must set method name
  conn->SetMethodName(methodname);

  // write data into sending buffer, when using libevent we don't need loop send
  if ( conn->AddToWriteBuffer(buf, sizeof(buf)) == false ) {
      LOG_TRACE("Write data Error");
      return;
  }

  // Connect to server with given address
  if ( conn->Connect(addr_) == false ) {
      LOG_TRACE("Connect Error");
      return;
  }

  // prepaere workers_thread to send and recv data
  std::function<void()> worker_conn =
          std::bind(&Connection::Dispatch, conn);

  // add workers to thread pool to send and recv data
  //ThreadManager::GetInstance().AddTask(worker_conn);
  //RpcClient::client_threads_.AddTask(worker_conn);
  ThreadManager::GetClientThreadPool().AddTask(worker_conn);
}

void RpcChannel::Close() {

}


}  // namespace networking
}  // namespace peloton
