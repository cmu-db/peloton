/**
*
* example: echo server
*
*/

#include "abstract_service.pb.h"
#include "rpc_server.h"
#include "peloton_endpoint.h"

#include <iostream>

namespace peloton {
namespace message {

class PelotonService: public AbstractPelotonService {
public:
	PelotonService() {
	}
	;

	// implements AbstractPelotonService ------------------------------------------

	void TransactionInit(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionInitRequest* request,
			::peloton::message::TransactionInitResponse* response,
			::google::protobuf::Closure* done);
	void TransactionWork(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionWorkRequest* request,
			::peloton::message::TransactionWorkResponse* response,
			::google::protobuf::Closure* done);
	void TransactionPrefetch(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionPrefetchResult* request,
			::peloton::message::TransactionPrefetchAcknowledgement* response,
			::google::protobuf::Closure* done);
	void TransactionMap(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionMapRequest* request,
			::peloton::message::TransactionMapResponse* response,
			::google::protobuf::Closure* done);
	void TransactionReduce(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionReduceRequest* request,
			::peloton::message::TransactionReduceResponse* response,
			::google::protobuf::Closure* done);
	void TransactionPrepare(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionPrepareRequest* request,
			::peloton::message::TransactionPrepareResponse* response,
			::google::protobuf::Closure* done);
	void TransactionFinish(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionFinishRequest* request,
			::peloton::message::TransactionFinishResponse* response,
			::google::protobuf::Closure* done);
	void TransactionRedirect(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionRedirectRequest* request,
			::peloton::message::TransactionRedirectResponse* response,
			::google::protobuf::Closure* done);
	void TransactionDebug(::google::protobuf::RpcController* controller,
			const ::peloton::message::TransactionDebugRequest* request,
			::peloton::message::TransactionDebugResponse* response,
			::google::protobuf::Closure* done);
	void SendData(::google::protobuf::RpcController* controller,
			const ::peloton::message::SendDataRequest* request,
			::peloton::message::SendDataResponse* response,
			::google::protobuf::Closure* done);
	void Initialize(::google::protobuf::RpcController* controller,
			const ::peloton::message::InitializeRequest* request,
			::peloton::message::InitializeResponse* response,
			::google::protobuf::Closure* done);
	void ShutdownPrepare(::google::protobuf::RpcController* controller,
			const ::peloton::message::ShutdownPrepareRequest* request,
			::peloton::message::ShutdownPrepareResponse* response,
			::google::protobuf::Closure* done);
	void Shutdown(::google::protobuf::RpcController* controller,
			const ::peloton::message::ShutdownRequest* request,
			::peloton::message::ShutdownResponse* response,
			::google::protobuf::Closure* done);
	void Heartbeat(::google::protobuf::RpcController* controller,
			const ::peloton::message::HeartbeatRequest* request,
			::peloton::message::HeartbeatResponse* response,
			::google::protobuf::Closure* done);
	void UnevictData(::google::protobuf::RpcController* controller,
			const ::peloton::message::UnevictDataRequest* request,
			::peloton::message::UnevictDataResponse* response,
			::google::protobuf::Closure* done);
	void TimeSync(::google::protobuf::RpcController* controller,
			const ::peloton::message::TimeSyncRequest* request,
			::peloton::message::TimeSyncResponse* response,
			::google::protobuf::Closure* done);
};

void StartPelotonService() {

	try {
		RpcServer rpc_server(PELOTON_ENDPOINT_ADDR);
		::google::protobuf::Service *service = new PelotonService();
		rpc_server.RegisterService(service);
		rpc_server.Start();
	} catch (peloton::message::exception& e) {
		std::cerr << "NN EXCEPTION : " << e.what() << std::endl;
	} catch (std::exception& e) {
		std::cerr << "STD EXCEPTION : " << e.what() << std::endl;
	} catch (...) {
		std::cerr << "UNTRAPPED EXCEPTION " << std::endl;
	}
}

}  // namespace message
}  // namespace peloton
