//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_client.h
//
// Identification: src/backend/networking/peloton_client.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/networking/rpc_channel.h"
#include "backend/networking/rpc_controller.h"
#include "backend/networking/abstract_service.pb.h"
#include "backend/networking/peloton_endpoint.h"

#include <google/protobuf/stubs/common.h>

namespace peloton {
namespace networking {

class PelotonClient {
 public:
  PelotonClient(const char* url) {
    channel_ = new RpcChannel(url);
    controller_ = new RpcController();
    stub_ = new AbstractPelotonService::Stub(channel_);
  };

  ~PelotonClient() {
    delete channel_;
    delete controller_;
    delete stub_;
  }

  // same rpc interface except for controller and callback function

  void TransactionInit(const TransactionInitRequest* request,
                       TransactionInitResponse* response) {
    stub_->TransactionInit(controller_, request, response, NULL);
  }

  void TransactionWork(const TransactionWorkRequest* request,
                       TransactionWorkResponse* response) {
    stub_->TransactionWork(controller_, request, response, NULL);
  }

  void TransactionPrefetch(const TransactionPrefetchResult* request,
                           TransactionPrefetchAcknowledgement* response) {
    stub_->TransactionPrefetch(controller_, request, response, NULL);
  }

  void TransactionMap(const TransactionMapRequest* request,
                      TransactionMapResponse* response) {
    stub_->TransactionMap(controller_, request, response, NULL);
  }

  void TransactionReduce(const TransactionReduceRequest* request,
                         TransactionReduceResponse* response) {
    stub_->TransactionReduce(controller_, request, response, NULL);
  }

  void TransactionPrepare(const TransactionPrepareRequest* request,
                          TransactionPrepareResponse* response) {
    stub_->TransactionPrepare(controller_, request, response, NULL);
  }

  void TransactionFinish(const TransactionFinishRequest* request,
                         TransactionFinishResponse* response) {
    stub_->TransactionFinish(controller_, request, response, NULL);
  }

  void TransactionRedirect(const TransactionRedirectRequest* request,
                           TransactionRedirectResponse* response) {
    stub_->TransactionRedirect(controller_, request, response, NULL);
  }

  void TransactionDebug(const TransactionDebugRequest* request,
                        TransactionDebugResponse* response) {
    stub_->TransactionDebug(controller_, request, response, NULL);
  }

  void SendData(const SendDataRequest* request, SendDataResponse* response) {
    stub_->SendData(controller_, request, response, NULL);
  }

  void Initialize(const InitializeRequest* request,
                  InitializeResponse* response) {
    stub_->Initialize(controller_, request, response, NULL);
  }

  void ShutdownPrepare(const ShutdownPrepareRequest* request,
                       ShutdownPrepareResponse* response) {
    stub_->ShutdownPrepare(controller_, request, response, NULL);
  }

  void Shutdown(const ShutdownRequest* request, ShutdownResponse* response) {
    stub_->Shutdown(controller_, request, response, NULL);
  }

  void Heartbeat(const HeartbeatRequest* request, HeartbeatResponse* response) {
    google::protobuf::Closure* callback = google::protobuf::NewCallback(&Call);
    stub_->Heartbeat(controller_, request, response, callback);
  }

  void UnevictData(const UnevictDataRequest* request,
                   UnevictDataResponse* response) {
    stub_->UnevictData(controller_, request, response, NULL);
  }

  void TimeSync(const TimeSyncRequest* request, TimeSyncResponse* response) {
    stub_->TimeSync(controller_, request, response, NULL);
  }

 private:
  static void Call() { LOG_TRACE("This is a backcall"); }

  RpcChannel* channel_;
  RpcController* controller_;
  AbstractPelotonService::Stub* stub_;
};

}  // namespace networking
}  // namespace peloton
