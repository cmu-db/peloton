//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// peloton_client.h
//
// Identification: src/backend/message/peloton_client.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "rpc_channel.h"
#include "rpc_controller.h"
#include "abstract_service.pb.h"
#include "peloton_endpoint.h"

#include <google/protobuf/stubs/common.h>
#include <iostream>

namespace peloton {
namespace message {

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

  void TransactionInit(const ::peloton::message::TransactionInitRequest* request,
                       ::peloton::message::TransactionInitResponse* response) {
    stub_->TransactionInit(controller_, request, response, NULL);
  }

  void TransactionWork(const ::peloton::message::TransactionWorkRequest* request,
                       ::peloton::message::TransactionWorkResponse* response) {
    stub_->TransactionWork(controller_, request, response, NULL);
  }

  void TransactionPrefetch(const ::peloton::message::TransactionPrefetchResult* request,
                           ::peloton::message::TransactionPrefetchAcknowledgement* response) {
    stub_->TransactionPrefetch(controller_, request, response, NULL);
  }

  void TransactionMap(const ::peloton::message::TransactionMapRequest* request,
                      ::peloton::message::TransactionMapResponse* response) {
    stub_->TransactionMap(controller_, request, response, NULL);
  }

  void TransactionReduce(const ::peloton::message::TransactionReduceRequest* request,
                         ::peloton::message::TransactionReduceResponse* response) {
    stub_->TransactionReduce(controller_, request, response, NULL);
  }

  void TransactionPrepare(const ::peloton::message::TransactionPrepareRequest* request,
                          ::peloton::message::TransactionPrepareResponse* response) {
    stub_->TransactionPrepare(controller_, request, response, NULL);
  }

  void TransactionFinish(const ::peloton::message::TransactionFinishRequest* request,
                         ::peloton::message::TransactionFinishResponse* response) {
    stub_->TransactionFinish(controller_, request, response, NULL);
  }

  void TransactionRedirect(const ::peloton::message::TransactionRedirectRequest* request,
                           ::peloton::message::TransactionRedirectResponse* response) {
    stub_->TransactionRedirect(controller_, request, response, NULL);
  }

  void TransactionDebug(const ::peloton::message::TransactionDebugRequest* request,
                        ::peloton::message::TransactionDebugResponse* response) {
    stub_->TransactionDebug(controller_, request, response, NULL);
  }

  void SendData(const ::peloton::message::SendDataRequest* request,
                ::peloton::message::SendDataResponse* response) {
    stub_->SendData(controller_, request, response, NULL);
  }

  void Initialize(const ::peloton::message::InitializeRequest* request,
                  ::peloton::message::InitializeResponse* response) {
    stub_->Initialize(controller_, request, response, NULL);
  }

  void ShutdownPrepare(const ::peloton::message::ShutdownPrepareRequest* request,
                       ::peloton::message::ShutdownPrepareResponse* response) {
    stub_->ShutdownPrepare(controller_, request, response, NULL);
  }

  void Shutdown(const ::peloton::message::ShutdownRequest* request,
                ::peloton::message::ShutdownResponse* response) {
    stub_->Shutdown(controller_, request, response, NULL);
  }

  void Heartbeat(const ::peloton::message::HeartbeatRequest* request,
                 ::peloton::message::HeartbeatResponse* response) {
    google::protobuf::Closure* callback = google::protobuf::NewCallback(&Call);
    stub_->Heartbeat(controller_, request, response, callback);
  }

  void UnevictData(const ::peloton::message::UnevictDataRequest* request,
                   ::peloton::message::UnevictDataResponse* response) {
    stub_->UnevictData(controller_, request, response, NULL);
  }

  void TimeSync(const ::peloton::message::TimeSyncRequest* request,
                ::peloton::message::TimeSyncResponse* response) {
    stub_->TimeSync(controller_, request, response, NULL);
  }

 private:

  static void Call() {
    std::cout << "This is backcall:" << std::endl;
  }

  RpcChannel*       channel_;
  RpcController*    controller_;
  AbstractPelotonService::Stub* stub_;
};

}  // namespace message
}  // namespace peloton
