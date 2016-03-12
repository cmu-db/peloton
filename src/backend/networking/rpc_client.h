//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_client.h
//
// Identification: src/backend/message/rpc_client.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "rpc_type.h"
#include "rpc_controller.h"
#include "rpc_channel.h"
#include "abstract_service.pb.h"
#include "peloton_endpoint.h"

#include "backend/common/logger.h"

#include <google/protobuf/stubs/common.h>
#include <iostream>

namespace peloton {
namespace message {

//class RpcClientManager;

class RpcClient {
 public:
    RpcClient(const char* url);
    ~RpcClient();

    //static ThreadManager client_threads_;

 public:

  // TODO: same rpc interface except for controller and callback function. We might delete response in future

  void TransactionInit(const ::peloton::message::TransactionInitRequest* request,
                       ::peloton::message::TransactionInitResponse* response);

  void TransactionWork(const ::peloton::message::TransactionWorkRequest* request,
                       ::peloton::message::TransactionWorkResponse* response);

  void TransactionPrefetch(const ::peloton::message::TransactionPrefetchResult* request,
                           ::peloton::message::TransactionPrefetchAcknowledgement* response);

  void TransactionMap(const ::peloton::message::TransactionMapRequest* request,
                      ::peloton::message::TransactionMapResponse* response);

  void TransactionReduce(const ::peloton::message::TransactionReduceRequest* request,
                         ::peloton::message::TransactionReduceResponse* response);

  void TransactionPrepare(const ::peloton::message::TransactionPrepareRequest* request,
                          ::peloton::message::TransactionPrepareResponse* response);

  void TransactionFinish(const ::peloton::message::TransactionFinishRequest* request,
                         ::peloton::message::TransactionFinishResponse* response);

  void TransactionRedirect(const ::peloton::message::TransactionRedirectRequest* request,
                           ::peloton::message::TransactionRedirectResponse* response);

  void TransactionDebug(const ::peloton::message::TransactionDebugRequest* request,
                        ::peloton::message::TransactionDebugResponse* response);

  void SendData(const ::peloton::message::SendDataRequest* request,
                ::peloton::message::SendDataResponse* response);

  void Initialize(const ::peloton::message::InitializeRequest* request,
                  ::peloton::message::InitializeResponse* response);

  void ShutdownPrepare(const ::peloton::message::ShutdownPrepareRequest* request,
                       ::peloton::message::ShutdownPrepareResponse* response);

  void Shutdown(const ::peloton::message::ShutdownRequest* request,
                ::peloton::message::ShutdownResponse* response);

  void Heartbeat(const ::peloton::message::HeartbeatRequest* request,
                 ::peloton::message::HeartbeatResponse* response);

  void UnevictData(const ::peloton::message::UnevictDataRequest* request,
                   ::peloton::message::UnevictDataResponse* response);

  void TimeSync(const ::peloton::message::TimeSyncRequest* request,
                ::peloton::message::TimeSyncResponse* response);

 private:

  RpcChannel*       channel_;

  //TODO: controller might be moved out if needed
  RpcController*    controller_;
  AbstractPelotonService::Stub* stub_;

};

}  // namespace message
}  // namespace peloton
