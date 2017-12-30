//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_service.cpp
//
// Identification: src/network/peloton_service.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/service/peloton_service.h"
#include "network/service/peloton_endpoint.h"
#include "network/service/rpc_server.h"
#include "common/logger.h"
#include "common/macros.h"
#include "executor/plan_executor.h"
#include "planner/seq_scan_plan.h"
#include "storage/tile.h"
#include "storage/tuple.h"
#include "type/serializeio.h"
#include "type/serializer.h"
#include "common/internal_types.h"

#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <cinttypes>

namespace peloton {
namespace network {
namespace service {

void PelotonService::TransactionInit(
    ::google::protobuf::RpcController* controller,
    UNUSED_ATTRIBUTE const TransactionInitRequest* request,
    UNUSED_ATTRIBUTE TransactionInitResponse* response,
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
    UNUSED_ATTRIBUTE const TransactionWorkRequest* request,
    UNUSED_ATTRIBUTE TransactionWorkResponse* response,
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
    UNUSED_ATTRIBUTE const TransactionPrefetchResult* request,
    UNUSED_ATTRIBUTE TransactionPrefetchAcknowledgement* response,
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
    UNUSED_ATTRIBUTE const TransactionMapRequest* request,
    UNUSED_ATTRIBUTE TransactionMapResponse* response,
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
    UNUSED_ATTRIBUTE const TransactionReduceRequest* request,
    UNUSED_ATTRIBUTE TransactionReduceResponse* response,
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
    UNUSED_ATTRIBUTE const TransactionPrepareRequest* request,
    UNUSED_ATTRIBUTE TransactionPrepareResponse* response,
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
    UNUSED_ATTRIBUTE const TransactionFinishRequest* request,
    UNUSED_ATTRIBUTE TransactionFinishResponse* response,
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
    UNUSED_ATTRIBUTE const TransactionRedirectRequest* request,
    UNUSED_ATTRIBUTE TransactionRedirectResponse* response,
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
    UNUSED_ATTRIBUTE const TransactionDebugRequest* request,
    UNUSED_ATTRIBUTE TransactionDebugResponse* response,
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
                              UNUSED_ATTRIBUTE const SendDataRequest* request,
                              UNUSED_ATTRIBUTE SendDataResponse* response,
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

void PelotonService::Initialize(
    ::google::protobuf::RpcController* controller,
    UNUSED_ATTRIBUTE const InitializeRequest* request,
    UNUSED_ATTRIBUTE InitializeResponse* response,
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
    UNUSED_ATTRIBUTE const ShutdownPrepareRequest* request,
    UNUSED_ATTRIBUTE ShutdownPrepareResponse* response,
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
                              UNUSED_ATTRIBUTE const ShutdownRequest* request,
                              UNUSED_ATTRIBUTE ShutdownResponse* response,
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
                               UNUSED_ATTRIBUTE const HeartbeatRequest* request,
                               UNUSED_ATTRIBUTE HeartbeatResponse* response,
                               ::google::protobuf::Closure* done) {
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // If request is not null, this is a rpc  call, server should handle the
  // reqeust
  if (request != NULL) {
    LOG_TRACE("Received from client, sender site: %d, last_txn_id: %" PRId64,
              request->sender_site(), request->last_transaction_id());

    response->set_sender_site(9876);
    Status status = ABORT_SPECULATIVE;
    response->set_status(status);

    // if callback exist, run it
    if (done) {
      done->Run();
    }
  }
  // Here is for the client callback for Heartbeat
  else {
    // proecess the response
    LOG_TRACE("proecess the Heartbeat response");

    if (response->has_sender_site() == true) {
      LOG_TRACE("sender site: %d", response->sender_site());
    } else {
      LOG_ERROR("No response: site is null");
    }
  }
}

void PelotonService::UnevictData(
    ::google::protobuf::RpcController* controller,
    UNUSED_ATTRIBUTE const UnevictDataRequest* request,
    UNUSED_ATTRIBUTE UnevictDataResponse* response,
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
                              UNUSED_ATTRIBUTE const TimeSyncRequest* request,
                              UNUSED_ATTRIBUTE TimeSyncResponse* response,
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
 * This a QueryPlan Processing function. It is a framework right now. Different
 * query type can be added.
 */
void PelotonService::QueryPlan(::google::protobuf::RpcController* controller,
                               const QueryPlanExecRequest* request,
                               QueryPlanExecResponse* response,
                               ::google::protobuf::Closure* done) {
  // TODO: controller should be set, we probably use it in the future
  if (controller->Failed()) {
    std::string error = controller->ErrorText();
    LOG_TRACE("PelotonService with controller failed:%s ", error.c_str());
  }

  // If request is not null, this is a rpc  call, server should handle the
  // reqeust
  if (request != NULL) {
    LOG_TRACE("Received a queryplan");

    if (!request->has_plan_type()) {
      LOG_ERROR("Queryplan recived desen't have type");
      return;
    }

    // construct parameter list
    // int param_num = request->param_num();
    std::string param_list = request->param_list();
    ReferenceSerializeInput param_input(param_list.c_str(), param_list.size());
    std::vector<type::Value> params;
    // for (int it = 0; it < param_num; it++) {
    //  // TODO: Make sure why varlen_pool is used as parameter
    //  std::shared_ptr<type::VarlenPool> pool(new
    //  type::VarlenPool(BackendType::MM));
    //  Value value_item;
    //  value_item.DeserializeFromAllocateForStorage(param_input, pool.get());
    //  params.push_back(value_item);
    //}

    // construct TupleDesc
    // std::unique_ptr<tupleDesc> tuple_desc =
    // ParseTupleDescMsg(request->tuple_dec());

    PlanNodeType plan_type = static_cast<PlanNodeType>(request->plan_type());

    // TODO: We can add more plan type in this switch to process
    switch (plan_type) {
      case PlanNodeType::INVALID: {
        LOG_ERROR("Queryplan recived desen't have type");
        break;
      }

      case PlanNodeType::SEQSCAN: {
        LOG_TRACE("SEQSCAN revieved");
        std::string plan = request->plan();
        ReferenceSerializeInput input(plan.c_str(), plan.size());
        std::shared_ptr<peloton::planner::SeqScanPlan> ss_plan =
            std::make_shared<peloton::planner::SeqScanPlan>();
        ss_plan->DeserializeFrom(input);

        std::vector<std::unique_ptr<executor::LogicalTile>> logical_tile_list;
        int tuple_count = peloton::executor::PlanExecutor::ExecutePlan(
            ss_plan.get(), params, logical_tile_list);
        // Return result
        if (tuple_count < 0) {
          // ExecutePlan fails
          LOG_ERROR("ExecutePlan fails");
          return;
        }

        // Set the basic metadata
        response->set_tuple_count(tuple_count);
        response->set_tile_count(logical_tile_list.size());

        // Loop the logical_tile_list to set the response
        std::vector<std::unique_ptr<executor::LogicalTile>>::iterator it;
        for (it = logical_tile_list.begin(); it != logical_tile_list.end();
             it++) {
          // First materialize logicalTile to physical tile
          std::unique_ptr<storage::Tile> tile = (*it)->Materialize();

          // Then serialize physical tile
          CopySerializeOutput output_tiles;
          tile->SerializeTo(output_tiles, tile->GetActiveTupleCount());

          // Finally set the response, which be automatically sent back
          response->add_result(output_tiles.Data(), output_tiles.Size());

          // Debug
          LOG_TRACE("Tile content is: %s", tile->GetInfo().c_str());
        }

        break;
      }

      default: {
        LOG_ERROR("Queryplan recived :: Unsupported TYPE: %s",
                  PlanNodeTypeToString(plan_type).c_str());
        break;
      }
    }

    // If callback exist, run it
    if (done) {
      done->Run();
    }
  }
  // Here is for the client callback for response
  else {
    // Process the response
    LOG_TRACE("proecess the Query response");
    PL_ASSERT(response);

    UNUSED_ATTRIBUTE int tile_count = response->tile_count();
    UNUSED_ATTRIBUTE int result_size = response->result_size();
    PL_ASSERT(result_size == tile_count);

    for (int idx = 0; idx < result_size; idx++) {
      // Get the tile bytes
      std::string tile_bytes = response->result(idx);
      ReferenceSerializeInput tile_input(tile_bytes.c_str(), tile_bytes.size());

      // Create a tile or tuple that depends on our protocol.
      // Tuple is prefered, since it voids copying from tile again
      // But we should prepare schema before creating tuple/tile.
      // Can we get the schema from local catalog?
      // storage::Tile tile;
      storage::Tuple tuple;

      // TODO: Make sure why varlen_pool is used as parameter
      // std::shared_ptr<VarlenPool> var_pool(new VarlenPool(BackendType::MM));

      // Tile deserialization.
      // tile.DeserializeTuplesFrom(tile_input, var_pool);

      // We should remove tile header or no header when serialize, then use
      // tuple deserialize
      tuple.DeserializeWithHeaderFrom(tile_input);

      // Debug
      // LOG_TRACE("Recv a tile: %s", tile.GetInfo().c_str());
    }
  }
}

}  // namespace service
}  // namespace network
}  // namespace peloton
