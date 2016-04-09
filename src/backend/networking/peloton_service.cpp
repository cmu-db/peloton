//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_service.cpp
//
// Identification: src/backend/networking/peloton_service.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/networking/peloton_service.h"
#include "backend/networking/peloton_endpoint.h"
#include "backend/networking/rpc_server.h"
#include "backend/networking/rpc_utils.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"
#include "backend/common/serializer.h"
#include "backend/common/assert.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/bridge/dml/executor/plan_executor.h"

#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <iostream>

namespace peloton {
namespace networking {

void PelotonService::TransactionInit(
    ::google::protobuf::RpcController* controller,
     __attribute__((unused)) const TransactionInitRequest* request,
     __attribute__((unused)) TransactionInitResponse* response,
     ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::TransactionWork(
    ::google::protobuf::RpcController* controller,
     __attribute__((unused)) const TransactionWorkRequest* request,
     __attribute__((unused)) TransactionWorkResponse* response,
     ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::TransactionPrefetch(
    ::google::protobuf::RpcController* controller,
     __attribute__((unused)) const TransactionPrefetchResult* request,
     __attribute__((unused)) TransactionPrefetchAcknowledgement* response,
     ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::TransactionMap(
    ::google::protobuf::RpcController* controller,
     __attribute__((unused)) const TransactionMapRequest* request,
     __attribute__((unused)) TransactionMapResponse* response,
     ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::TransactionReduce(
    ::google::protobuf::RpcController* controller,
     __attribute__((unused)) const TransactionReduceRequest* request,
     __attribute__((unused)) TransactionReduceResponse* response,
     ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::TransactionPrepare(
    ::google::protobuf::RpcController* controller,
     __attribute__((unused)) const TransactionPrepareRequest* request,
     __attribute__((unused)) TransactionPrepareResponse* response,
     ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::TransactionFinish(
    ::google::protobuf::RpcController* controller,
     __attribute__((unused)) const TransactionFinishRequest* request,
     __attribute__((unused)) TransactionFinishResponse* response,
     ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::TransactionRedirect(
    ::google::protobuf::RpcController* controller,
     __attribute__((unused)) const TransactionRedirectRequest* request,
     __attribute__((unused)) TransactionRedirectResponse* response,
     ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::TransactionDebug(
    ::google::protobuf::RpcController* controller,
     __attribute__((unused)) const TransactionDebugRequest* request,
     __attribute__((unused)) TransactionDebugResponse* response,
     ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::SendData(::google::protobuf::RpcController* controller,
                              __attribute__((unused)) const SendDataRequest* request,
                              __attribute__((unused)) SendDataResponse* response,
                              ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::Initialize(::google::protobuf::RpcController* controller,
                                __attribute__((unused)) const InitializeRequest* request,
                                __attribute__((unused)) InitializeResponse* response,
                                ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::ShutdownPrepare(
    ::google::protobuf::RpcController* controller,
     __attribute__((unused)) const ShutdownPrepareRequest* request,
     __attribute__((unused)) ShutdownPrepareResponse* response,
     ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::Shutdown(::google::protobuf::RpcController* controller,
                              __attribute__((unused)) const ShutdownRequest* request,
                              __attribute__((unused)) ShutdownResponse* response,
                              ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::Heartbeat(::google::protobuf::RpcController* controller,
                               __attribute__((unused)) const HeartbeatRequest* request,
                               __attribute__((unused)) HeartbeatResponse* response,
                               ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  /*
   * If request is not null, this is a rpc  call, server should handle the
   * reqeust
   */
  if (request != NULL) {
    LOG_TRACE("Received from client, sender site: %d, last_txn_id: %ld",
              request->sender_site(), request->last_transaction_id());

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
      LOG_TRACE("sender site: %d", response->sender_site());
    } else {
      LOG_ERROR("No response: site is null");
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
    if (response->has_status() == true) {
      LOG_TRACE("Status: %d", response->status());
    } else {
      LOG_ERROR("No response: status is null");
    }
  }
  }
}

void PelotonService::UnevictData(::google::protobuf::RpcController* controller,
                                 __attribute__((unused)) const UnevictDataRequest* request,
                                 __attribute__((unused)) UnevictDataResponse* response,
                                 ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

void PelotonService::TimeSync(::google::protobuf::RpcController* controller,
                              __attribute__((unused)) const TimeSyncRequest* request,
                              __attribute__((unused)) TimeSyncResponse* response,
                              ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // if callback exist, run it
  if (done) {
    done->Run();
  }
}

/*
 * This a QueryPlan Processing function. It is a framework right now. Different query type can be added.
 */
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

        // construct parameter list
        int param_num = request->param_num();
        std::string param_list = request->param_list();
        ReferenceSerializeInputBE param_input(param_list.c_str(), param_list.size());
        std::vector<Value> params;
        for (int it = 0; it < param_num; it++) {
            // TODO: Make sure why varlen_pool is used as parameter
            std::shared_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
            Value value_item;
            value_item.DeserializeFromAllocateForStorage(param_input, pool.get());
            params.push_back(value_item);
        }

        // construct TupleDesc
        std::unique_ptr<tupleDesc> tuple_desc = ParseTupleDescMsg(request->tuple_dec());

        PlanNodeType plan_type = static_cast<PlanNodeType>(request->plan_type());

        // TODO: We can add more plan type in this switch to process
        switch (plan_type) {
            case PLAN_NODE_TYPE_INVALID: {
                LOG_ERROR("Queryplan recived desen't have type");
                break;
            }

            case PLAN_NODE_TYPE_SEQSCAN: {
                LOG_TRACE("SEQSCAN revieved");
                std::string plan = request->plan();
                ReferenceSerializeInputBE input(plan.c_str(), plan.size());
                std::shared_ptr<peloton::planner::SeqScanPlan> ss_plan =
                        std::make_shared<peloton::planner::SeqScanPlan>();
                ss_plan->DeserializeFrom(input);

                //peloton_status status =
                        peloton::bridge::PlanExecutor::ExecutePlan(ss_plan.get(),
                                                                    params,
                                                                    tuple_desc.get());
                // TODO: We should return the result here
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
        ASSERT(response);
    }
}

}  // namespace networking
}  // namespace peloton
