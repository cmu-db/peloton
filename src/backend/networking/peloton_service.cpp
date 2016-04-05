//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// peloton_service.cpp
//
// Identification: src/backend/networking/peloton_service.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/networking/peloton_service.h"
#include "backend/networking/peloton_endpoint.h"
#include "backend/networking/rpc_server.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/bridge/dml/executor/plan_executor.h"

#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <iostream>

namespace peloton {
namespace networking {

void PelotonService::TransactionInit(::google::protobuf::RpcController* controller,
        const TransactionInitRequest* request,
        TransactionInitResponse* response,
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
        const TransactionWorkRequest* request,
        TransactionWorkResponse* response,
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
        const TransactionPrefetchResult* request,
        TransactionPrefetchAcknowledgement* response,
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
        const TransactionMapRequest* request,
        TransactionMapResponse* response,
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
        const TransactionReduceRequest* request,
        TransactionReduceResponse* response,
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
        const TransactionPrepareRequest* request,
        TransactionPrepareResponse* response,
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
        const TransactionFinishRequest* request,
        TransactionFinishResponse* response,
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
        const TransactionRedirectRequest* request,
        TransactionRedirectResponse* response,
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
        const TransactionDebugRequest* request,
        TransactionDebugResponse* response,
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
        const SendDataRequest* request,
        SendDataResponse* response,
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
        const InitializeRequest* request,
        InitializeResponse* response,
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
        const ShutdownPrepareRequest* request,
        ShutdownPrepareResponse* response,
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
        const ShutdownRequest* request,
        ShutdownResponse* response,
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
        const HeartbeatRequest* request,
        HeartbeatResponse* response,
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
        const UnevictDataRequest* request,
        UnevictDataResponse* response,
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
        const TimeSyncRequest* request,
        TimeSyncResponse* response,
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
void PelotonService::QueryPlan(::google::protobuf::RpcController* controller,
        const QueryPlanExecRequest* request,
        QueryPlanExecResponse* response,
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
        LOG_TRACE("Received a queryplan");

        if (!request->has_plan_type()) {
            LOG_ERROR("Queryplan recived desen't have type");
            return;
        }

        PlanNodeType plan_type = static_cast<PlanNodeType>(request->plan_type());

        switch (plan_type) {
            case PLAN_NODE_TYPE_INVALID: {
                LOG_ERROR("Queryplan recived desen't have type");
                break;
            }

            case PLAN_NODE_TYPE_SEQSCAN: {
                LOG_ERROR("SEQSCAN revieved");
                std::string plan = request->plan();
                ReferenceSerializeInputBE input(plan.c_str(), plan.size());
                std::shared_ptr<peloton::planner::SeqScanPlan> ss_plan =
                        std::make_shared<peloton::planner::SeqScanPlan>();
                ss_plan->DeserializeFrom(input);

                //ParamListInfo param_list;
                //TupleDesc tuple_desc;

                //peloton_status status =
                //        peloton::bridge::PlanExecutor::ExecutePlan(ss_plan.get(),
                //                                                    param_list,
                //                                                    tuple_desc);
                break;
            }

            default: {
              LOG_ERROR("Queryplan recived :: Unsupported TYPE: %u ",
                      plan_type);
              break;
            }
        }

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
        LOG_TRACE("proecess the Query response");
        std::cout << response;
    }
}

}  // namespace networking
}  // namespace peloton
