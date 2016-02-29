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

#include "backend/message/rpc_server.h"
#include "backend/message/rpc_controller.h"
#include "backend/message/nanomsg.h"
#include "backend/common/logger.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/stubs/common.h>
#include <city.h>
#include <iostream>

namespace peloton {
namespace message {

RpcServer::RpcServer(const char* url) :
    socket_tcp_(AF_SP_RAW, NN_REP),
    socket_inproc_(AF_SP_RAW, NN_REQ),
    socket_tcp_id_(socket_tcp_.Bind(url)),
    socket_inproc_id_(socket_inproc_.Bind(RECV_QUEUE)),
    worker_thread1_(),
    worker_thread2_(),
    worker_thread3_(),
    worker_thread4_(),
    worker_thread5_() {

}

RpcServer::~RpcServer() {

    if(worker_thread1_.joinable()) worker_thread1_.join();
    if(worker_thread2_.joinable()) worker_thread2_.join();
    if(worker_thread3_.joinable()) worker_thread3_.join();
    if(worker_thread4_.joinable()) worker_thread4_.join();
    if(worker_thread5_.joinable()) worker_thread5_.join();
    RemoveService();
    Close();
}

void RpcServer::EndPoint(const char* url) {

    socket_tcp_.Bind(url);
}

/*
 * @brief service is implemented by programer, suach as from peloton service interface
 * Therefore, a service has several methods. These methods can be registered using
 * this function
 */
void RpcServer::RegisterService(google::protobuf::Service *service) {

  // Get the service descriptor
  const google::protobuf::ServiceDescriptor *descriptor = service->GetDescriptor();

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
    uint64_t hash = CityHash64(methodname.c_str(), methodname.length());
    RpcMethodMap::const_iterator iter = rpc_method_map_.find(hash);
    if (iter == rpc_method_map_.end())
      rpc_method_map_[hash] = rpc_method;
    }
}
/*
void RpcServer::StartSimple() {

  uint64_t opcode = 0;

  while (1) {

	// Receive message
    char* buf = NULL;
    int bytes = socket_.Receive(&buf, NN_MSG, 0);
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

    // Must free the buf since we use NN_MSG flag
    freemsg(buf);

    // Invoke the corresponding rpc method
    rpc_method->service_->CallMethod(method, NULL, request, response, NULL);

    // Send back the response message. The message has been set up when executing rpc method
    size_t msg_len = response->ByteSize();
    buf = (char*)peloton::message::allocmsg(msg_len, 0);
    response->SerializeToArray(buf, msg_len);

    // We can use NN_MSG instead of msg_len here, but using msg_len is still ok
    socket_.Send(buf, msg_len, 0);
    delete request;
    delete response;

    // Must free the buf since it is created using nanomsg::allocmsg()
    freemsg(buf);
  }
}
*/

void RpcServer::Start() {

    worker_thread1_ = std::thread(&RpcServer::Worker, this, "this is worker_thread1");
    worker_thread2_ = std::thread(&RpcServer::Worker, this, "this is worker_thread2");
    worker_thread3_ = std::thread(&RpcServer::Worker, this, "this is worker_thread3");
    worker_thread4_ = std::thread(&RpcServer::Worker, this, "this is worker_thread4");
    worker_thread5_ = std::thread(&RpcServer::Worker, this, "this is worker_thread5");

    int rc = device(socket_tcp_.GetSocket(), socket_inproc_.GetSocket());

    std:: cout << "rc: " << rc << std::endl;

}

void RpcServer::Worker(const char* debuginfo) {

    std::cout << debuginfo << std::endl;

    NanoMsg socket(AF_SP, NN_REP);

    socket.Connect(RECV_QUEUE);

    uint64_t opcode = 0;

    while (1) {
      // Receive message
      char* buf = NULL;
      int bytes = socket.Receive(&buf, NN_MSG, 0);
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

      // Must free the buf since we use NN_MSG flag
      freemsg(buf);

      // Invoke the corresponding rpc method
      google::protobuf::Closure* callback = google::protobuf::internal::NewCallback(&Callback);
      RpcController controller;
      rpc_method->service_->CallMethod(method, &controller, request, response, callback);

      // TODO: controller should be set within rpc method
      if (controller.Failed()) {
          std::string error = controller.ErrorText();
          LOG_TRACE( "RpcServer with controller failed:%s ", error.c_str() );
      }

      // Send back the response message. The message has been set up when executing rpc method
      size_t msg_len = response->ByteSize();
      buf = (char*)peloton::message::allocmsg(msg_len, 0);
      response->SerializeToArray(buf, msg_len);

      // We can use NN_MSG instead of msg_len here, but using msg_len is still ok
      socket.Send(buf, msg_len, 0);
      delete request;
      delete response;

      // Must free the buf since it is created using nanomsg::allocmsg()
      freemsg(buf);
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

    if (socket_tcp_id_ > 0) {
      socket_tcp_.Shutdown(socket_tcp_id_);
      socket_tcp_id_ = 0;
    }

    if (socket_inproc_id_ > 0) {
      socket_inproc_.Shutdown(socket_inproc_id_);
      socket_inproc_id_ = 0;
    }
}

}  // namespace message
}  // namespace peloton
