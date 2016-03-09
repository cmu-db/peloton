//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_server.h
//
// Identification: src/backend/message/rpc_server.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "backend/networking/rpc_server.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/stubs/common.h>

namespace peloton {
namespace networking {

RpcServer::RpcServer(const char* url) :
      socket_id_(socket_.Bind(url)),
      worker_thread_() {
}

RpcServer::~RpcServer() {

  if(worker_thread_.joinable()) worker_thread_.join();
  RemoveService();
  Close();

}

void RpcServer::EndPoint(const char* url) {

  socket_.Bind(url);
}

/*
 * @brief service is implemented by programer, suach as from peloton service interface
 * Therefore, a service has several methods. These methods can be registered using
 * this function
 */
void RpcServer::RegisterService(google::protobuf::Service *service) {

  // Get the service descriptor
  const google::protobuf::ServiceDescriptor *descriptor = service->GetDescriptor();
  std::hash<std::string> string_hash_fn;

  /*
   * Put all of the method names (descriptors)， msg types into rpc_method_map_
   * For example, peloton service has HeartBeat method, its
   * request msg type is HeartbeatRequest
   * response msg type is HeartbeatResponse
   */
  for (int i = 0; i < descriptor->method_count(); ++i) {

    // Get the method descriptor
    const google::protobuf::MethodDescriptor *method = descriptor->method(i);

    // Get request and response type through method descriptor
    const google::protobuf::Message *request = &service->GetRequestPrototype(method);
    const google::protobuf::Message *response = &service->GetResponsePrototype(method);

    // Create the corresponding method structure
    RpcMethod *rpc_method = new RpcMethod(service, request, response, method);

    // Put the method into rpc_method_map_: hashcode-->method
    std::string methodname = std::string(method->full_name());
    // TODO:
    //uint64_t hash = CityHash64(methodname.c_str(), methodname.length());
    size_t hash = string_hash_fn(methodname);
    RpcMethodMap::const_iterator iter = rpc_method_map_.find(hash);
    if (iter == rpc_method_map_.end())
      rpc_method_map_[hash] = rpc_method;
  }
}

void RpcServer::StartSimple() {

  uint64_t opcode = 0;

  while (1) {

    // Receive message
    char* buf = NULL;
    int bytes = socket_.Receive(&buf, 0, 0);
    if (bytes <= 0) continue;

    // Get the hashcode of the rpc method
    memcpy((char*)(&opcode), buf, sizeof(opcode));

    // Get the method iter from local map
    RpcMethodMap::const_iterator iter = rpc_method_map_.find(opcode);
    if (iter == rpc_method_map_.end()) {
      continue;
    }

    // Get the rpc method meta info: method descriptor
    RpcMethod *rpc_method = iter->second;
    const google::protobuf::MethodDescriptor *method = rpc_method->method_;

    // Get request and response type and create them
    google::protobuf::Message *request = rpc_method->request_->New();
    google::protobuf::Message *response = rpc_method->response_->New();

    // Deserialize the receiving message
    request->ParseFromString(buf + sizeof(opcode));

    // Must free the buf
    delete buf;

    // Invoke the corresponding rpc method
    rpc_method->service_->CallMethod(method, NULL, request, response, NULL);

    // Send back the response message. The message has been set up when executing rpc method
    size_t msg_len = response->ByteSize();
    buf = new char[msg_len];
    response->SerializeToArray(buf, msg_len);

    // We can use NN_MSG instead of msg_len here, but using msg_len is still ok
    socket_.Send(buf, msg_len, 0);
    delete request;
    delete response;

    // Must free the buf
    delete buf;
  }
}


void RpcServer::Start() {

  //	worker_thread_ = std::thread(&RpcServer::Worker, this, "this is worker_thread");

  uint64_t opcode = 0;

  while (1) {
    // Receive message
    char* buf = NULL;
    int bytes = socket_.Receive(&buf, 0, 0);
    if (bytes <= 0) continue;

    // Get the hashcode of the rpc method
    memcpy((char*)(&opcode), buf, sizeof(opcode));

    // Get the method iter from local map
    RpcMethodMap::const_iterator iter = rpc_method_map_.find(opcode);

    if (iter == rpc_method_map_.end()) {
      continue;
    }

    // Get the rpc method meta info: method descriptor
    RpcMethod *rpc_method = iter->second;

    // Get request and response type and create them
    google::protobuf::Message *request = rpc_method->request_->New();

    // Deserialize the receiving message
    request->ParseFromString(buf + sizeof(opcode));

    RecvItem item;
    item.socket = &socket_;
    item.method = rpc_method;
    item.request = request;


    recv_queue_.Push(item);

    // Must free the buf
    delete buf;

    //Worker("Function call");
  }
}

void RpcServer::Worker(const char* debuginfo) {

  std::cout << debuginfo << std::endl;

  RecvItem item = recv_queue_.Pop();

  NanoMsg* socket = item.socket;
  RpcMethod* rpc_method = item.method;
  google::protobuf::Message* request = item.request;
  google::protobuf::Message *response = rpc_method->response_->New();
  const google::protobuf::MethodDescriptor *method = rpc_method->method_;

  // Invoke the corresponding rpc method
  rpc_method->service_->CallMethod(method, NULL, request, response, NULL);

  // Send back the response message. The message has been set up when executing rpc method
  size_t msg_len = response->ByteSize();
  char* buf = NULL;
  buf = new char[msg_len];
  response->SerializeToArray(buf, msg_len);

  try {
    std::cout << debuginfo << ": prepare to send" << std::endl;
    // We can use NN_MSG instead of msg_len here, but using msg_len is still ok
    socket->Send(buf, msg_len, 0);
    std::cout << debuginfo << ": after send" << std::endl;

  } catch (std::exception& e) {
    std::cerr << "STD EXCEPTION : " << e.what() << std::endl;
    delete request;
    delete response;
    // Must free the buf
    delete buf;
  } catch (...) {
    std::cerr << "UNTRAPPED EXCEPTION " << std::endl;
    delete request;
    delete response;
    // Must free the buf
    delete buf;
  }

}

void RpcServer::RemoveService() {

  RpcMethodMap::iterator iter;

  for (RpcMethodMap::iterator it = rpc_method_map_.begin(); it != rpc_method_map_.end();) {
    RpcMethod *rpc_method = it->second;
    ++it;
    delete rpc_method;
  }
}

void RpcServer::Close() {

  if (socket_id_ > 0) {
    socket_.Shutdown(socket_id_);
    socket_id_ = 0;
  }
}

}  // namespace networking
}  // namespace peloton
