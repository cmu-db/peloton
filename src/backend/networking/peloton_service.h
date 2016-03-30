//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_service.h
//
// Identification: src/backend/networking/peloton_service.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/networking/abstract_service.pb.h"
#include "backend/networking/rpc_server.h"
#include "backend/networking/peloton_endpoint.h"

#include <iostream>

//===--------------------------------------------------------------------===//
// Implements AbstractPelotonService
//===--------------------------------------------------------------------===//

namespace peloton {
namespace networking {

class PelotonService : public AbstractPelotonService {
 public:

  virtual void TransactionInit(::google::protobuf::RpcController* controller,
                               const TransactionInitRequest* request,
                               TransactionInitResponse* response,
                               ::google::protobuf::Closure* done);
  virtual void TransactionWork(::google::protobuf::RpcController* controller,
                               const TransactionWorkRequest* request,
                               TransactionWorkResponse* response,
                               ::google::protobuf::Closure* done);
  virtual void TransactionPrefetch(
      ::google::protobuf::RpcController* controller,
       const TransactionPrefetchResult* request,
       TransactionPrefetchAcknowledgement* response,
       ::google::protobuf::Closure* done);
  virtual void TransactionMap(::google::protobuf::RpcController* controller,
                              const TransactionMapRequest* request,
                              TransactionMapResponse* response,
                              ::google::protobuf::Closure* done);
  virtual void TransactionReduce(::google::protobuf::RpcController* controller,
                                 const TransactionReduceRequest* request,
                                 TransactionReduceResponse* response,
                                 ::google::protobuf::Closure* done);
  virtual void TransactionPrepare(::google::protobuf::RpcController* controller,
                                  const TransactionPrepareRequest* request,
                                  TransactionPrepareResponse* response,
                                  ::google::protobuf::Closure* done);
  virtual void TransactionFinish(::google::protobuf::RpcController* controller,
                                 const TransactionFinishRequest* request,
                                 TransactionFinishResponse* response,
                                 ::google::protobuf::Closure* done);
  virtual void TransactionRedirect(
      ::google::protobuf::RpcController* controller,
       const TransactionRedirectRequest* request,
       TransactionRedirectResponse* response, ::google::protobuf::Closure* done);
  virtual void TransactionDebug(::google::protobuf::RpcController* controller,
                                const TransactionDebugRequest* request,
                                TransactionDebugResponse* response,
                                ::google::protobuf::Closure* done);
  virtual void SendData(::google::protobuf::RpcController* controller,
                        const SendDataRequest* request,
                        SendDataResponse* response,
                        ::google::protobuf::Closure* done);
  virtual void Initialize(::google::protobuf::RpcController* controller,
                          const InitializeRequest* request,
                          InitializeResponse* response,
                          ::google::protobuf::Closure* done);
  virtual void ShutdownPrepare(::google::protobuf::RpcController* controller,
                               const ShutdownPrepareRequest* request,
                               ShutdownPrepareResponse* response,
                               ::google::protobuf::Closure* done);
  virtual void Shutdown(::google::protobuf::RpcController* controller,
                        const ShutdownRequest* request,
                        ShutdownResponse* response,
                        ::google::protobuf::Closure* done);
  virtual void Heartbeat(::google::protobuf::RpcController* controller,
                         const HeartbeatRequest* request,
                         HeartbeatResponse* response,
                         ::google::protobuf::Closure* done);
  virtual void UnevictData(::google::protobuf::RpcController* controller,
                           const UnevictDataRequest* request,
                           UnevictDataResponse* response,
                           ::google::protobuf::Closure* done);
  virtual void TimeSync(::google::protobuf::RpcController* controller,
                        const TimeSyncRequest* request,
                        TimeSyncResponse* response,
                        ::google::protobuf::Closure* done);
};

}  // namespace networking
}  // namespace peloton
