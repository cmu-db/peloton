//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_server.cpp
//
// Identification: src/network/service/rpc_server.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/service/rpc_server.h"
#include "network/service/rpc_controller.h"
#include "network/service/connection_manager.h"
#include "common/logger.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/stubs/common.h>

#include <iostream>
#include <functional>

namespace peloton {
namespace network {
namespace service {

RpcServer::RpcServer(const int port) : listener_(port) {
  // We have listener here, so we also have event base
  ConnectionManager::GetInstance().ResterRpcServer(this);
}

RpcServer::~RpcServer() { RemoveService(); }

/*
 * @brief service is implemented by programer, suach as from peloton service
 * interface
 * Therefore, a service has several methods. These methods can be registered
 * using
 * this function
 */
bool RpcServer::RegisterService(google::protobuf::Service *service) {
  // Get the service descriptor
  const google::protobuf::ServiceDescriptor *descriptor =
      service->GetDescriptor();
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
    const google::protobuf::Message *request =
        &service->GetRequestPrototype(method);
    const google::protobuf::Message *response =
        &service->GetResponsePrototype(method);

    // Create the corresponding method structure
    RpcMethod *rpc_method = new RpcMethod(service, request, response, method);

    // Put the method into rpc_method_map_: hashcode-->method
    std::string methodname = std::string(method->full_name());
    // TODO:
    // uint64_t hash = CityHash64(methodname.c_str(), methodname.length());

    // although the return type is size_t, again we should specify the size of
    // the type
    uint64_t hash = string_hash_fn(methodname);
    RpcMethodMap::const_iterator iter = rpc_method_map_.find(hash);
    if (iter == rpc_method_map_.end()) {
      auto pair = std::make_pair(hash, rpc_method);
      rpc_method_map_.insert(pair);
    }
  }

  return true;
}

void RpcServer::Start() { listener_.Run(this); }

void RpcServer::RemoveService() {
  for (RpcMethodMap::iterator iter = rpc_method_map_.begin();
       iter != rpc_method_map_.end(); iter++) {
    RpcMethod *rpc_method = iter->second;

    if (rpc_method != NULL) {
      delete rpc_method;
    }
  }
}

RpcMethod *RpcServer::FindMethod(uint64_t opcode) {
  // Get the method iter from local map
  RpcMethodMap::const_iterator iter = rpc_method_map_.find(opcode);
  if (iter == rpc_method_map_.end()) {
    return NULL;
  }

  // Get the rpc method meta info: method descriptor
  RpcMethod *rpc_method = iter->second;

  return rpc_method;
}

// get listener
Listener *RpcServer::GetListener() { return &listener_; }

}  // namespace service
}  // namespace network
}  // namespace peloton
