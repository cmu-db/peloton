//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_server.h
//
// Identification: src/include/network/service/rpc_server.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <thread>
#include <map>

#include "common/logger.h"
#include "rpc_method.h"
#include "tcp_listener.h"

namespace peloton {
namespace network {
namespace service {

class RpcServer {
  typedef std::map<uint64_t, RpcMethod *> RpcMethodMap;

 public:
  RpcServer(const int port);
  ~RpcServer();

  // start
  void Start();

  // register service
  bool RegisterService(google::protobuf::Service *service);

  // find a rpcmethod
  RpcMethod *FindMethod(uint64_t opcode);

  // get listener
  Listener *GetListener();

 private:
  // remove all services. It is only invoked by destroy constructor
  void RemoveService();

  // the rpc function can call this to execute something
  static void Callback() { LOG_TRACE("This is server backcall"); }

  RpcMethodMap rpc_method_map_;

  Listener listener_;

  //  MessageQueue<RecvItem>   recv_queue_;
};

}  // namespace service
}  // namespace network
}  // namespace peloton
