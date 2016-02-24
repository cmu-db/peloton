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

#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/stubs/common.h>

namespace peloton {
namespace message {

struct RpcMethod {
public:
	RpcMethod(google::protobuf::Service *service,
	          const google::protobuf::Message *request,
	          const google::protobuf::Message *response,
	          const google::protobuf::MethodDescriptor *method)
		: service_(service),
		  request_(request),
		  response_(response),
		  method_(method) {
	}

	google::protobuf::Service *service_;
	const google::protobuf::Message *request_;
	const google::protobuf::Message *response_;
	const google::protobuf::MethodDescriptor *method_;
};

}  // namespace message
}  // namespace peloton
