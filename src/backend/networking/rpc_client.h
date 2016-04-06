//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_client.h
//
// Identification: src/backend/networking/rpc_client.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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
namespace networking {

// class RpcClientManager;

class RpcClient {
 public:
  RpcClient(const char* url);
  ~RpcClient();

  // static ThreadManager client_threads_;

 public:
  // TODO: same rpc interface except for controller and callback function. We
  // might delete response in future

  void TransactionInit(const TransactionInitRequest* request,
                       TransactionInitResponse* response);

  void TransactionWork(const TransactionWorkRequest* request,
                       TransactionWorkResponse* response);

  void TransactionPrefetch(const TransactionPrefetchResult* request,
                           TransactionPrefetchAcknowledgement* response);

  void TransactionMap(const TransactionMapRequest* request,
                      TransactionMapResponse* response);

  void TransactionReduce(const TransactionReduceRequest* request,
                         TransactionReduceResponse* response);

  void TransactionPrepare(const TransactionPrepareRequest* request,
                          TransactionPrepareResponse* response);

  void TransactionFinish(const TransactionFinishRequest* request,
                         TransactionFinishResponse* response);

  void TransactionRedirect(const TransactionRedirectRequest* request,
                           TransactionRedirectResponse* response);

  void TransactionDebug(const TransactionDebugRequest* request,
                        TransactionDebugResponse* response);

  void SendData(const SendDataRequest* request, SendDataResponse* response);

  void Initialize(const InitializeRequest* request,
                  InitializeResponse* response);

  void ShutdownPrepare(const ShutdownPrepareRequest* request,
                       ShutdownPrepareResponse* response);

  void Shutdown(const ShutdownRequest* request, ShutdownResponse* response);

  void Heartbeat(const HeartbeatRequest* request, HeartbeatResponse* response);

  void UnevictData(const UnevictDataRequest* request,
                   UnevictDataResponse* response);

  void TimeSync(const TimeSyncRequest* request, TimeSyncResponse* response);

 private:
  RpcChannel* channel_;

  // TODO: controller might be moved out if needed
  RpcController* controller_;
  AbstractPelotonService::Stub* stub_;
};

}  // namespace networking
}  // namespace peloton
