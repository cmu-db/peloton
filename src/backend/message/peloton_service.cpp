//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// nanomsg.cpp
//
// Identification: src/backend/message/peloton_service.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "peloton_service.h"
#include "peloton_endpoint.h"
#include "rpc_server.h"

#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <iostream>

namespace peloton {
namespace message {

//void PelotonService::TransactionInit(::google::protobuf::RpcController* controller,
//		const ::peloton::message::TransactionInitRequest* request,
//		::peloton::message::TransactionInitResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::TransactionWork(::google::protobuf::RpcController* controller,
//		const ::peloton::message::TransactionWorkRequest* request,
//		::peloton::message::TransactionWorkResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::TransactionPrefetch(::google::protobuf::RpcController* controller,
//		const ::peloton::message::TransactionPrefetchResult* request,
//		::peloton::message::TransactionPrefetchAcknowledgement* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::TransactionMap(::google::protobuf::RpcController* controller,
//		const ::peloton::message::TransactionMapRequest* request,
//		::peloton::message::TransactionMapResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::TransactionReduce(::google::protobuf::RpcController* controller,
//		const ::peloton::message::TransactionReduceRequest* request,
//		::peloton::message::TransactionReduceResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::TransactionPrepare(::google::protobuf::RpcController* controller,
//		const ::peloton::message::TransactionPrepareRequest* request,
//		::peloton::message::TransactionPrepareResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::TransactionFinish(::google::protobuf::RpcController* controller,
//		const ::peloton::message::TransactionFinishRequest* request,
//		::peloton::message::TransactionFinishResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::TransactionRedirect(::google::protobuf::RpcController* controller,
//		const ::peloton::message::TransactionRedirectRequest* request,
//		::peloton::message::TransactionRedirectResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::TransactionDebug(::google::protobuf::RpcController* controller,
//		const ::peloton::message::TransactionDebugRequest* request,
//		::peloton::message::TransactionDebugResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::SendData(::google::protobuf::RpcController* controller,
//		const ::peloton::message::SendDataRequest* request,
//		::peloton::message::SendDataResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::Initialize(::google::protobuf::RpcController* controller,
//		const ::peloton::message::InitializeRequest* request,
//		::peloton::message::InitializeResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::ShutdownPrepare(::google::protobuf::RpcController* controller,
//		const ::peloton::message::ShutdownPrepareRequest* request,
//		::peloton::message::ShutdownPrepareResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::Shutdown(::google::protobuf::RpcController* controller,
//		const ::peloton::message::ShutdownRequest* request,
//		::peloton::message::ShutdownResponse* response,
//		::google::protobuf::Closure* done) {}

void PelotonService::Heartbeat(::google::protobuf::RpcController* controller,
		const ::peloton::message::HeartbeatRequest* request,
		::peloton::message::HeartbeatResponse* response,
		::google::protobuf::Closure* done) {

	std::cerr << "Received1: " << request->sender_site() << request->last_transaction_id() << std::endl;
	response->set_sender_site(9876);
	Status status = ABORT_SPECULATIVE;
	response->set_status(status);

	if (done) {
		done->Run();
	}

	//TODO: use controller here
	std::cout << controller << std::endl;
}

//void PelotonService::UnevictData(::google::protobuf::RpcController* controller,
//		const ::peloton::message::UnevictDataRequest* request,
//		::peloton::message::UnevictDataResponse* response,
//		::google::protobuf::Closure* done) {}
//
//void PelotonService::TimeSync(::google::protobuf::RpcController* controller,
//		const ::peloton::message::TimeSyncRequest* request,
//		::peloton::message::TimeSyncResponse* response,
//		::google::protobuf::Closure* done) {}

}  // namespace message
}  // namespace peloton
