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

#pragma once

#include "nanomsg.h"
#include "message_queue.h"
#include "rpc_method.h"

#include <string>
#include <thread>
#include <map>

namespace peloton {
namespace message {

class RpcServer {

  typedef std::map<uint64_t, RpcMethod*> RpcMethodMap;
  typedef struct RecvItem {
    NanoMsg*                    socket;
    RpcMethod* 					method;
    google::protobuf::Message* 	request;
  } QueueItem;

public:
  RpcServer(const char* url);
  ~RpcServer();

  // add more endpoints
  void EndPoint(const char* url);

  // start
  void Start();
  void StartSimple();

  // Multiple woker threads
  void Worker(const char* debuginfo);

//  std::thread WorkerThread(const char* debuginfo) {
//    return std::thread([=] { Worker(debuginfo); });
//  }

  // register a service
  void RegisterService(google::protobuf::Service *service);

  // remove all services
  void RemoveService();

  // close
  void Close();

private:

  NanoMsg        socket_;
  int            socket_id_;
  RpcMethodMap   rpc_method_map_;

  std::thread    worker_thread_;
  MessageQueue<RecvItem>   recv_queue_;
};

}  // namespace message
}  // namespace peloton
