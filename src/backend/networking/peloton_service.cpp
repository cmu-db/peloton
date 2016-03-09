//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// peloton_service.cpp
//
// Identification: src/backend/message/peloton_service.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <iostream>

#include "backend/networking/peloton_service.h"
#include "backend/networking/peloton_endpoint.h"
#include "backend/networking/rpc_server.h"

namespace peloton {
namespace networking {

void PelotonService::TransactionInit(::google::protobuf::RpcController* controller,
        const TransactionInitRequest* request,
        TransactionInitResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::TransactionWork(::google::protobuf::RpcController* controller,
        const TransactionWorkRequest* request,
        TransactionWorkResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::TransactionPrefetch(::google::protobuf::RpcController* controller,
        const TransactionPrefetchResult* request,
        TransactionPrefetchAcknowledgement* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::TransactionMap(::google::protobuf::RpcController* controller,
        const TransactionMapRequest* request,
        TransactionMapResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::TransactionReduce(::google::protobuf::RpcController* controller,
        const TransactionReduceRequest* request,
        TransactionReduceResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::TransactionPrepare(::google::protobuf::RpcController* controller,
        const TransactionPrepareRequest* request,
        TransactionPrepareResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::TransactionFinish(::google::protobuf::RpcController* controller,
        const TransactionFinishRequest* request,
        TransactionFinishResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::TransactionRedirect(::google::protobuf::RpcController* controller,
        const TransactionRedirectRequest* request,
        TransactionRedirectResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::TransactionDebug(::google::protobuf::RpcController* controller,
        const TransactionDebugRequest* request,
        TransactionDebugResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::SendData(::google::protobuf::RpcController* controller,
        const SendDataRequest* request,
        SendDataResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::Initialize(::google::protobuf::RpcController* controller,
        const InitializeRequest* request,
        InitializeResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::ShutdownPrepare(::google::protobuf::RpcController* controller,
        const ShutdownPrepareRequest* request,
        ShutdownPrepareResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::Shutdown(::google::protobuf::RpcController* controller,
        const ShutdownRequest* request,
        ShutdownResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::Heartbeat(::google::protobuf::RpcController* controller,
        const HeartbeatRequest* request,
        HeartbeatResponse* response,
        ::google::protobuf::Closure* done) {

    std::cerr << "Received from client: " <<
        "sender site: " << request->sender_site() <<
        "last_txn_id: " << request->last_transaction_id() <<
         std::endl;

    response->set_sender_site(9876);
    Status status = ABORT_SPECULATIVE;
    response->set_status(status);

    if (done) {
        done->Run();
    }

    //TODO: use controller here
    std::cout << "controller: " << controller << std::endl;
}

void PelotonService::UnevictData(::google::protobuf::RpcController* controller,
        const UnevictDataRequest* request,
        UnevictDataResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}

void PelotonService::TimeSync(::google::protobuf::RpcController* controller,
        const TimeSyncRequest* request,
        TimeSyncResponse* response,
        ::google::protobuf::Closure* done) {

    //TODO: use controller here
    std::cout << controller << request << response << done << std::endl;
}
/*
void StartPelotonService() {

	::google::protobuf::Service* service = NULL;

	try {
		RpcServer rpc_server(PELOTON_ENDPOINT_ADDR);
		service = new PelotonService();
		rpc_server.RegisterService(service);
		rpc_server.Start();
	} catch (peloton::message::exception& e) {
		std::cerr << "NN EXCEPTION : " << e.what() << std::endl;
		delete service;
	} catch (std::exception& e) {
		std::cerr << "STD EXCEPTION : " << e.what() << std::endl;
		delete service;
	} catch (...) {
		std::cerr << "UNTRAPPED EXCEPTION " << std::endl;
		delete service;
	}
}
*/
}  // namespace networking
}  // namespace peloton
