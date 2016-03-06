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
#include "backend/common/thread_manager.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/stubs/common.h>
#include <iostream>
#include <functional>

namespace peloton {
namespace message {

RpcServer::RpcServer(const char* url) :
    socket_tcp_(AF_SP_RAW, NN_REP),
    socket_inproc_(AF_SP_RAW, NN_REQ),
    socket_tcp_id_(socket_tcp_.Bind(url)),
    socket_inproc_id_(socket_inproc_.Bind(RECV_QUEUE))
{}

RpcServer::~RpcServer() {

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

void RpcServer::Forward() {
    LOG_TRACE ("Start forwarding messages between sockets: %d and %d", socket_tcp_.GetSocket(), socket_inproc_.GetSocket());
    device(socket_tcp_.GetSocket(), socket_inproc_.GetSocket());
}

void RpcServer::Start() {
/*
    // prepaere workers
    std::function<void()> worker1 = std::bind(&RpcServer::Worker, this, "this is worker_thread1");
    std::function<void()> worker2 = std::bind(&RpcServer::Worker, this, "this is worker_thread2");
    std::function<void()> worker3 = std::bind(&RpcServer::Worker, this, "this is worker_thread3");
    std::function<void()> worker4 = std::bind(&RpcServer::Worker, this, "this is worker_thread4");
    std::function<void()> worker5 = std::bind(&RpcServer::Worker, this, "this is worker_thread5");

    // add workers to thread pool
    ThreadManager::GetInstance().AddTask(worker1);
    ThreadManager::GetInstance().AddTask(worker2);
    ThreadManager::GetInstance().AddTask(worker3);
    ThreadManager::GetInstance().AddTask(worker4);
    ThreadManager::GetInstance().AddTask(worker5);
*/
    // start receiving and forwarding
    std::function<void()> forward = std::bind(&RpcServer::Forward, this);
    // add workers to thread pool
    ThreadManager::GetInstance().AddTask(forward);

    pollfd pfd [1];
    pfd [0].fd = socket_tcp_.GetSocket();
    pfd [0].events = NN_POLLIN;

    // Loop listening the socket_inproc_
    while (true) {
        int rc = poll (pfd, 1, 1000);
        if (rc == 0) {
            LOG_TRACE ("Timeout when check fd: %d out", pfd [0].fd);
            continue;
        }
        if (rc == -1) {
            LOG_TRACE ("Error when check fd: %d out", pfd [0].fd);
            exit (1);
        }
        if (pfd [0].revents & NN_POLLIN) {
            LOG_TRACE ("Server: Message can be received from fd: %d", pfd [0].fd);

            // prepaere workers
            std::function<void()> worker =
                    std::bind(&RpcServer::Worker, this, "this is worker_thread");

            // add workers to thread pool
            ThreadManager::GetInstance().AddTask(worker);
        }
    }

}

void RpcServer::Worker(const char* debuginfo) {

    std::cout << debuginfo << std::endl;

    NanoMsg socket(AF_SP, NN_REP);

    socket.Connect(RECV_QUEUE);

    uint64_t opcode = 0;

    int bytes = 0;

    while (bytes <= 0) {
        // Receive message
        char* buf = NULL;
        bytes = socket.Receive(&buf, NN_MSG, 0);
        if (bytes <= 0) {
            LOG_TRACE("receive nothing and continue");
            continue;
        }
        // Get the hashcode of the rpc method
        memcpy((char*) (&opcode), buf, sizeof(opcode));

        // Get the method iter from local map
        RpcMethodMap::const_iterator iter = rpc_method_map_.find(opcode);
        if (iter == rpc_method_map_.end()) {
            LOG_TRACE("No method found");
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
        google::protobuf::Closure* callback =
                google::protobuf::internal::NewCallback(&Callback);
        RpcController controller;
        rpc_method->service_->CallMethod(method, &controller, request, response,
                callback);

        // TODO: controller should be set within rpc method
        if (controller.Failed()) {
            std::string error = controller.ErrorText();
            LOG_TRACE( "RpcServer with controller failed:%s ", error.c_str());
        }

        // Send back the response message. The message has been set up when executing rpc method
        size_t msg_len = response->ByteSize();
        buf = (char*) peloton::message::allocmsg(msg_len, 0);
        response->SerializeToArray(buf, msg_len);

        // We can use NN_MSG instead of msg_len here, but using msg_len is still ok
        LOG_TRACE("Server:Before send back");
        socket.Send(buf, msg_len, 0);
        LOG_TRACE("Server:After send back");;

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
