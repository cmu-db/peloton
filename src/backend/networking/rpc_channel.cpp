//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_channel.cpp
//
// Identification: src/backend/networking/rpc_channel.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/networking/rpc_type.h"
#include "backend/networking/rpc_client.h"
#include "backend/networking/rpc_channel.h"
#include "backend/networking/rpc_controller.h"
#include "backend/networking/tcp_connection.h"
#include "backend/networking/connection_manager.h"
#include "backend/common/thread_manager.h"
#include "backend/common/logger.h"

#include <google/protobuf/descriptor.h>

#include <iostream>
#include <functional>

namespace peloton {
namespace networking {

RpcChannel::RpcChannel(const std::string& url) : addr_(url) {}

RpcChannel::~RpcChannel() { Close(); }

////////////////////////////////////////////////////////////////////////////////
//                  Request message structure:
// --Header:  message length (Type+Opcode+request),      uint32_t (4bytes)
// --Type:    message type: REQUEST or RESPONSE          uint16_t (2bytes)
// --Opcode:  std::hash(methodname)-->Opcode,            uint64_t (8bytes)
// --Content: the serialization result of protobuf       Header-8-2
//
// TODO: We did not add checksum code in this version     ///////////////

/*
 * Channel is only invoked by protobuf rpc client. So this method is sending
 * request msg
 */
void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            __attribute__((unused))
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done) {
  assert(request != nullptr);
  /*  run call back function */
  if (done != NULL) {
    done->Run();
  }

  /* Get the rpc function name */
  std::string methodname = std::string(method->full_name());
  std::hash<std::string> string_hash_fn;

  /*  Set the type */
  uint16_t type = MSG_TYPE_REQ;

  /*  Get the hashcode for the rpc function name */
  /*  we use unit64_t because we should specify the exact length */
  uint64_t opcode = string_hash_fn(methodname);

  /* prepare the sending buf */
  uint32_t msg_len = request->ByteSize() + OPCODELEN + TYPELEN;

  /* total length of the message: header length (4bytes) + message length
   * (8bytes + ...) */
  assert(HEADERLEN == sizeof(msg_len));
  char buf[HEADERLEN + msg_len];

  /* copy the header into the buf */
  memcpy(buf, &msg_len, sizeof(msg_len));

  /* copy the type into the buf, following the header */
  assert(TYPELEN == sizeof(type));
  memcpy(buf + HEADERLEN, &type, TYPELEN);

  /*  copy the hashcode into the buf, following the type */
  assert(OPCODELEN == sizeof(opcode));
  memcpy(buf + HEADERLEN + TYPELEN, &opcode, OPCODELEN);

  /*  call protobuf to serialize the request message into sending buf */
  request->SerializeToArray(buf + +HEADERLEN + TYPELEN + OPCODELEN,
                            request->ByteSize());

  /*
   * GET a connection to process the rpc send and recv. If there is a associated
   * connection
   * it will be returned. If not, a new connection will be created and connect
   * to server
   */
  Connection* conn = ConnectionManager::GetInstance().CreateConn(addr_);

  /* Connect to server with given address */
  if (conn == NULL) {
    LOG_TRACE("Can't get connection");

    // rpc client use this info to decide whether re-send the message
    controller->SetFailed("Connect Error");

    return;
  }

  /* write data into sending buffer, when using libevent we don't need loop send
   */
  if (conn->AddToWriteBuffer(buf, sizeof(buf)) == false) {
    LOG_TRACE("Write data Error");
    return;
  }
}

void RpcChannel::Close() {}

}  // namespace networking
}  // namespace peloton
