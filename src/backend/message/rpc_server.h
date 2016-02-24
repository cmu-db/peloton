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
#include "rpc_method.h"

#include <string>
#include <map>

namespace peloton {
namespace message {

class RpcServer {

	typedef std::map<uint64_t, RpcMethod*> RpcMethodMap;

public:
	RpcServer(const char* url);
	~RpcServer();

	// add more endpoints
	void EndPoint(const char* url);

	// start
	void Start();

	// register a service
	void RegisterService(google::protobuf::Service *service);

	// remove all services
	void RemoveService();

	// close
	void Close();

private:

	NanoMsg socket_;
	int socket_id_;
	RpcMethodMap rpc_method_map_;
};

}  // namespace message
}  // namespace peloton
