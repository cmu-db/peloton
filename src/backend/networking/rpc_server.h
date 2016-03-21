//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_server.h
//
// Identification: src/backend/networking/rpc_server.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "backend/common/thread_manager.h"
#include "backend/networking/rpc_method.h"
#include "tcp_listener.h"

#include <iostream>
#include <string>
#include <thread>
#include <map>

namespace peloton {
namespace networking {

class RpcServer {

  typedef std::map<uint64_t, RpcMethod*> RpcMethodMap;

  /*
  typedef struct RecvItem {
    RpcMethod*                  method;
    google::protobuf::Message*  request;
  } QueueItem;
  */

public:
  RpcServer(const int port);
  ~RpcServer();

  // start
  void Start();

  // register service
  bool RegisterService(google::protobuf::Service *service);

  // find a rpcmethod
  RpcMethod* FindMethod(uint64_t opcode);

  // get listener
  Listener* GetListener();

private:

  // remove all services. It is only invoked by destroy constructor
  void RemoveService();

  // the rpc function can call this to execute something
  static void Callback() {
    std::cout << "This is server backcall" << std::endl;
  }

  RpcMethodMap   rpc_method_map_;

  Listener listener_;

//  MessageQueue<RecvItem>   recv_queue_;
};

}  // namespace networking
}  // namespace peloton
