//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_server.h
//
// Identification: src/backend/message/rpc_server.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/message/rpc_method.h"
#include "tcp_listener.h"

#include <iostream>
#include <string>
#include <thread>
#include <map>

namespace peloton {
namespace message {

class RpcServer {

  typedef std::map<uint64_t, RpcMethod*> RpcMethodMap;

  /*
  typedef struct RecvItem {
    RpcMethod* 					method;
    google::protobuf::Message* 	request;
  } QueueItem;
  */

public:
  RpcServer(const int port);
  ~RpcServer();

  // start
  void Start();

  // register a service
  void RegisterService(google::protobuf::Service *service);

  // remove all services
  void RemoveService();

  RpcMethod* FindMethod(uint64_t opcode);

  // close
  void Close();

private:

  // Multiple woker threads
  //void Worker(const char* debuginfo);

  // the rpc function can call this to execute something
  static void Callback() {
    std::cout << "This is server backcall" << std::endl;
  }

//  NanoMsg        socket_tcp_;
//  NanoMsg        socket_inproc_;
//  int            socket_tcp_id_;
//  int            socket_inproc_id_;

  RpcMethodMap   rpc_method_map_;

  Listener listener_;

//  MessageQueue<RecvItem>   recv_queue_;
};

}  // namespace message
}  // namespace peloton
