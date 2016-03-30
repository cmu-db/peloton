//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_client.cpp
//
// Identification: src/backend/networking/rpc_client.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "rpc_client.h"
#include "abstract_service.pb.h"

namespace peloton {
namespace networking {

#define CLIENT_THREADS 1

// ThreadManager RpcClient::client_threads_(CLIENT_THREADS);

RpcClient::RpcClient(const char* url)
    : channel_(new RpcChannel(url)),
      controller_(new RpcController()),
      stub_(new AbstractPelotonService::Stub(channel_)){};

RpcClient::~RpcClient() {
  delete channel_;
  delete controller_;
  delete stub_;
}

void RpcClient::TransactionInit(const TransactionInitRequest* request,
                                TransactionInitResponse* response) {
  stub_->TransactionInit(controller_, request, response, NULL);
}

void RpcClient::TransactionWork(const TransactionWorkRequest* request,
                                TransactionWorkResponse* response) {
  stub_->TransactionWork(controller_, request, response, NULL);
}

void RpcClient::TransactionPrefetch(
    const TransactionPrefetchResult* request,
    TransactionPrefetchAcknowledgement* response) {
  stub_->TransactionPrefetch(controller_, request, response, NULL);
}

void RpcClient::TransactionMap(const TransactionMapRequest* request,
                               TransactionMapResponse* response) {
  stub_->TransactionMap(controller_, request, response, NULL);
}

void RpcClient::TransactionReduce(const TransactionReduceRequest* request,
                                  TransactionReduceResponse* response) {
  stub_->TransactionReduce(controller_, request, response, NULL);
}

void RpcClient::TransactionPrepare(const TransactionPrepareRequest* request,
                                   TransactionPrepareResponse* response) {
  stub_->TransactionPrepare(controller_, request, response, NULL);
}

void RpcClient::TransactionFinish(const TransactionFinishRequest* request,
                                  TransactionFinishResponse* response) {
  stub_->TransactionFinish(controller_, request, response, NULL);
}

void RpcClient::TransactionRedirect(const TransactionRedirectRequest* request,
                                    TransactionRedirectResponse* response) {
  stub_->TransactionRedirect(controller_, request, response, NULL);
}

void RpcClient::TransactionDebug(const TransactionDebugRequest* request,
                                 TransactionDebugResponse* response) {
  stub_->TransactionDebug(controller_, request, response, NULL);
}

void RpcClient::SendData(const SendDataRequest* request,
                         SendDataResponse* response) {
  stub_->SendData(controller_, request, response, NULL);
}

void RpcClient::Initialize(const InitializeRequest* request,
                           InitializeResponse* response) {
  stub_->Initialize(controller_, request, response, NULL);
}

void RpcClient::ShutdownPrepare(const ShutdownPrepareRequest* request,
                                ShutdownPrepareResponse* response) {
  stub_->ShutdownPrepare(controller_, request, response, NULL);
}

void RpcClient::Shutdown(const ShutdownRequest* request,
                         ShutdownResponse* response) {
  stub_->Shutdown(controller_, request, response, NULL);
}

void RpcClient::Heartbeat(const HeartbeatRequest* request,
                          HeartbeatResponse* response) {
  // google::protobuf::Closure* callback =
  // google::protobuf::internal::NewCallback(&HearbeatCallback);
  // controller_->SetRpcClientManager(&client_manager_);
  stub_->Heartbeat(controller_, request, response, NULL);
}

void RpcClient::UnevictData(const UnevictDataRequest* request,
                            UnevictDataResponse* response) {
  stub_->UnevictData(controller_, request, response, NULL);
}

void RpcClient::TimeSync(const TimeSyncRequest* request,
                         TimeSyncResponse* response) {
  stub_->TimeSync(controller_, request, response, NULL);
}

}  // namespace networking
}  // namespace peloton
