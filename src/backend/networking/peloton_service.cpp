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

#include "backend/message/peloton_service.h"
#include "backend/message/peloton_endpoint.h"
#include "backend/message/rpc_server.h"
#include "backend/common/logger.h"

#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <iostream>

namespace peloton {
namespace message {

void PelotonService::TransactionInit(::google::protobuf::RpcController* controller,
        const ::peloton::message::TransactionInitRequest* request,
        ::peloton::message::TransactionInitResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::TransactionWork(::google::protobuf::RpcController* controller,
        const ::peloton::message::TransactionWorkRequest* request,
        ::peloton::message::TransactionWorkResponse* response,
        ::google::protobuf::Closure* done) {
    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::TransactionPrefetch(::google::protobuf::RpcController* controller,
        const ::peloton::message::TransactionPrefetchResult* request,
        ::peloton::message::TransactionPrefetchAcknowledgement* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::TransactionMap(::google::protobuf::RpcController* controller,
        const ::peloton::message::TransactionMapRequest* request,
        ::peloton::message::TransactionMapResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::TransactionReduce(::google::protobuf::RpcController* controller,
        const ::peloton::message::TransactionReduceRequest* request,
        ::peloton::message::TransactionReduceResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::TransactionPrepare(::google::protobuf::RpcController* controller,
        const ::peloton::message::TransactionPrepareRequest* request,
        ::peloton::message::TransactionPrepareResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::TransactionFinish(::google::protobuf::RpcController* controller,
        const ::peloton::message::TransactionFinishRequest* request,
        ::peloton::message::TransactionFinishResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::TransactionRedirect(::google::protobuf::RpcController* controller,
        const ::peloton::message::TransactionRedirectRequest* request,
        ::peloton::message::TransactionRedirectResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::TransactionDebug(::google::protobuf::RpcController* controller,
        const ::peloton::message::TransactionDebugRequest* request,
        ::peloton::message::TransactionDebugResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::SendData(::google::protobuf::RpcController* controller,
        const ::peloton::message::SendDataRequest* request,
        ::peloton::message::SendDataResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::Initialize(::google::protobuf::RpcController* controller,
        const ::peloton::message::InitializeRequest* request,
        ::peloton::message::InitializeResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::ShutdownPrepare(::google::protobuf::RpcController* controller,
        const ::peloton::message::ShutdownPrepareRequest* request,
        ::peloton::message::ShutdownPrepareResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::Shutdown(::google::protobuf::RpcController* controller,
        const ::peloton::message::ShutdownRequest* request,
        ::peloton::message::ShutdownResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::Heartbeat(::google::protobuf::RpcController* controller,
        const ::peloton::message::HeartbeatRequest* request,
        ::peloton::message::HeartbeatResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }


    /*
     * If request is not null, this is a rpc  call, server should handle the reqeust
     */
    if (request != NULL) {

        LOG_TRACE("Received from client, sender site: %d, last_txn_id: %lld",
                request->sender_site(),
                request->last_transaction_id());

        response->set_sender_site(9876);
        Status status = ABORT_SPECULATIVE;
        response->set_status(status);

        // if callback exist, run it
        if (done) {
            done->Run();
        }
    }

    /*
     * Here is for the client callback for Heartbeat
     */
    else {
        // proecess the response
        LOG_TRACE("proecess the Heartbeat response");

        if (response->has_sender_site() == true) {
            std::cout << "sender site: " << response->sender_site() << std::endl;
        } else {
            std::cout << "No response: site is null" << std::endl;
        }

        if (response->has_status() == true) {
            std::cout << "Status: " << response->status() << std::endl;
        } else {
            std::cout << "No response: status is null" << std::endl;
        }
    }


}

void PelotonService::UnevictData(::google::protobuf::RpcController* controller,
        const ::peloton::message::UnevictDataRequest* request,
        ::peloton::message::UnevictDataResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
}

void PelotonService::TimeSync(::google::protobuf::RpcController* controller,
        const ::peloton::message::TimeSyncRequest* request,
        ::peloton::message::TimeSyncResponse* response,
        ::google::protobuf::Closure* done) {

    // TODO: controller should be set, we probably use it in the future
    if (controller->Failed()) {
        std::string error = controller->ErrorText();
        LOG_TRACE( "PelotonService with controller failed:%s ", error.c_str() );
    }

    // TODO: process request and set response
    std::cout << request << response;

    // if callback exist, run it
    if (done) {
        done->Run();
    }
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
}  // namespace message
}  // namespace peloton
