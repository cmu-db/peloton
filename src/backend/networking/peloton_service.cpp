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
#include "backend/common/logger.h"

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

    if (response->has_status() == true) {
      LOG_TRACE("Status: %d", response->status());
    } else {
      LOG_ERROR("No response: status is null");
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

}  // namespace networking
}  // namespace peloton
