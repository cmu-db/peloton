//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// pelton_service.h
//
// Identification: src/backend/message/pelton_service.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/message/abstract_service.pb.h"
#include "backend/message/rpc_server.h"
#include "backend/message/peloton_endpoint.h"

#include <iostream>

namespace peloton {
namespace message {

class PelotonService : public AbstractPelotonService {
public:

	// implements AbstractPelotonService ------------------------------------------

	virtual void TransactionInit(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionInitRequest* request,
			::peloton::message::TransactionInitResponse* response,
			::google::protobuf::Closure* done);
	virtual void TransactionWork(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionWorkRequest* request,
			::peloton::message::TransactionWorkResponse* response,
			::google::protobuf::Closure* done);
	virtual void TransactionPrefetch(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionPrefetchResult* request,
			::peloton::message::TransactionPrefetchAcknowledgement* response,
			::google::protobuf::Closure* done);
	virtual void TransactionMap(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionMapRequest* request,
			::peloton::message::TransactionMapResponse* response,
			::google::protobuf::Closure* done);
	virtual void TransactionReduce(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionReduceRequest* request,
			::peloton::message::TransactionReduceResponse* response,
			::google::protobuf::Closure* done);
	virtual void TransactionPrepare(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionPrepareRequest* request,
			::peloton::message::TransactionPrepareResponse* response,
			::google::protobuf::Closure* done);
	virtual void TransactionFinish(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionFinishRequest* request,
			::peloton::message::TransactionFinishResponse* response,
			::google::protobuf::Closure* done);
	virtual void TransactionRedirect(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionRedirectRequest* request,
			::peloton::message::TransactionRedirectResponse* response,
			::google::protobuf::Closure* done);
	virtual void TransactionDebug(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionDebugRequest* request,
			::peloton::message::TransactionDebugResponse* response,
			::google::protobuf::Closure* done);
	virtual void SendData(::google::protobuf::RpcController* controller,
			const ::peloton::message::SendDataRequest* request,
			::peloton::message::SendDataResponse* response,
			::google::protobuf::Closure* done);
	virtual void Initialize(::google::protobuf::RpcController* controller,
			const ::peloton::message::InitializeRequest* request,
			::peloton::message::InitializeResponse* response,
			::google::protobuf::Closure* done);
	virtual void ShutdownPrepare(::google::protobuf::RpcController* controller,
			const ::peloton::message::ShutdownPrepareRequest* request,
			::peloton::message::ShutdownPrepareResponse* response,
			::google::protobuf::Closure* done);
	virtual void Shutdown(::google::protobuf::RpcController* controller,
			const ::peloton::message::ShutdownRequest* request,
			::peloton::message::ShutdownResponse* response,
			::google::protobuf::Closure* done);
	virtual void Heartbeat(::google::protobuf::RpcController* controller,
			const ::peloton::message::HeartbeatRequest* request,
			::peloton::message::HeartbeatResponse* response,
			::google::protobuf::Closure* done);
	virtual void UnevictData(::google::protobuf::RpcController* controller,
			const ::peloton::message::UnevictDataRequest* request,
			::peloton::message::UnevictDataResponse* response,
			::google::protobuf::Closure* done);
	virtual void TimeSync(::google::protobuf::RpcController* controller,
			const ::peloton::message::TimeSyncRequest* request,
			::peloton::message::TimeSyncResponse* response,
			::google::protobuf::Closure* done);
};

//void StartPelotonService();

}  // namespace message
}  // namespace peloton
