//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_client.cpp
//
// Identification: /peloton/src/backend/message/rpc_client.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "rpc_client.h"

namespace peloton {
namespace message {

#define CLIENT_THREADS 1

ThreadManager RpcClient::client_threads_(CLIENT_THREADS);

RpcClient::RpcClient(const char* url) :
       channel_ (new RpcChannel(url)),
       controller_ (new RpcController()),
       stub_ (new AbstractPelotonService::Stub(channel_)) {
 };

RpcClient::~RpcClient() {
   delete channel_;
   delete controller_;
   delete stub_;
 }

 void RpcClient::TransactionInit(const ::peloton::message::TransactionInitRequest* request,
                      ::peloton::message::TransactionInitResponse* response) {
   stub_->TransactionInit(controller_, request, response, NULL);
 }

 void RpcClient::TransactionWork(const ::peloton::message::TransactionWorkRequest* request,
                      ::peloton::message::TransactionWorkResponse* response) {
   stub_->TransactionWork(controller_, request, response, NULL);
 }

 void RpcClient::TransactionPrefetch(const ::peloton::message::TransactionPrefetchResult* request,
                          ::peloton::message::TransactionPrefetchAcknowledgement* response) {
   stub_->TransactionPrefetch(controller_, request, response, NULL);
 }

 void RpcClient::TransactionMap(const ::peloton::message::TransactionMapRequest* request,
                     ::peloton::message::TransactionMapResponse* response) {
   stub_->TransactionMap(controller_, request, response, NULL);
 }

 void RpcClient::TransactionReduce(const ::peloton::message::TransactionReduceRequest* request,
                        ::peloton::message::TransactionReduceResponse* response) {
   stub_->TransactionReduce(controller_, request, response, NULL);
 }

 void RpcClient::TransactionPrepare(const ::peloton::message::TransactionPrepareRequest* request,
                         ::peloton::message::TransactionPrepareResponse* response) {
   stub_->TransactionPrepare(controller_, request, response, NULL);
 }

 void RpcClient::TransactionFinish(const ::peloton::message::TransactionFinishRequest* request,
                        ::peloton::message::TransactionFinishResponse* response) {
   stub_->TransactionFinish(controller_, request, response, NULL);
 }

 void RpcClient::TransactionRedirect(const ::peloton::message::TransactionRedirectRequest* request,
                          ::peloton::message::TransactionRedirectResponse* response) {
   stub_->TransactionRedirect(controller_, request, response, NULL);
 }

 void RpcClient::TransactionDebug(const ::peloton::message::TransactionDebugRequest* request,
                       ::peloton::message::TransactionDebugResponse* response) {
   stub_->TransactionDebug(controller_, request, response, NULL);
 }

 void RpcClient::SendData(const ::peloton::message::SendDataRequest* request,
               ::peloton::message::SendDataResponse* response) {
   stub_->SendData(controller_, request, response, NULL);
 }

 void RpcClient::Initialize(const ::peloton::message::InitializeRequest* request,
                 ::peloton::message::InitializeResponse* response) {
   stub_->Initialize(controller_, request, response, NULL);
 }

 void RpcClient::ShutdownPrepare(const ::peloton::message::ShutdownPrepareRequest* request,
                      ::peloton::message::ShutdownPrepareResponse* response) {
   stub_->ShutdownPrepare(controller_, request, response, NULL);
 }

 void RpcClient::Shutdown(const ::peloton::message::ShutdownRequest* request,
               ::peloton::message::ShutdownResponse* response) {
   stub_->Shutdown(controller_, request, response, NULL);
 }

 void RpcClient::Heartbeat(const ::peloton::message::HeartbeatRequest* request,
                ::peloton::message::HeartbeatResponse* response) {
   //google::protobuf::Closure* callback = google::protobuf::internal::NewCallback(&HearbeatCallback);
   //controller_->SetRpcClientManager(&client_manager_);
   stub_->Heartbeat(controller_, request, response, NULL);
 }

 void RpcClient::UnevictData(const ::peloton::message::UnevictDataRequest* request,
                  ::peloton::message::UnevictDataResponse* response) {
   stub_->UnevictData(controller_, request, response, NULL);
 }

 void RpcClient::TimeSync(const ::peloton::message::TimeSyncRequest* request,
               ::peloton::message::TimeSyncResponse* response) {
   stub_->TimeSync(controller_, request, response, NULL);
 }


}  // namespace message
}  // namespace peloton
