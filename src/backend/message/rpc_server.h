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

#include "backend/message/nanomsg.h"
#include "backend/message/rpc_method.h"

#include <string>
#include <thread>
#include <map>

namespace peloton {
namespace message {

#define RECV_QUEUE "inproc://recv_queue"

class RpcServer {

  typedef std::map<uint64_t, RpcMethod*> RpcMethodMap;

  /*
  typedef struct RecvItem {
    RpcMethod* 					method;
    google::protobuf::Message* 	request;
  } QueueItem;
  */

public:
  RpcServer(const char* url);
  ~RpcServer();

  // add more endpoints
  void EndPoint(const char* url);

  // start
  void Start();
//  void StartSimple();

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

  NanoMsg        socket_tcp_;
  NanoMsg        socket_inproc_;
  int            socket_tcp_id_;
  int            socket_inproc_id_;
  RpcMethodMap   rpc_method_map_;

  std::thread    worker_thread1_;
  std::thread    worker_thread2_;
  std::thread    worker_thread3_;
  std::thread    worker_thread4_;
  std::thread    worker_thread5_;
//  MessageQueue<RecvItem>   recv_queue_;
};

}  // namespace message
}  // namespace peloton
