//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// protocol_handler.h
//
// Identification: src/include/network/protocol_handler.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "network/protocol_handler.h"

#include <boost/algorithm/string.hpp>

namespace peloton {
namespace network {

  ProtocolHandler::ProtocolHandler(tcop::TrafficCop *traffic_cop, logging::WalLogManager *log_manager) {
    this->traffic_cop_ = traffic_cop;
    this->log_manager_ = log_manager;
  }

  ProtocolHandler::~ProtocolHandler() {}


  /* Manage the startup packet */
  //  bool ManageStartupPacket();
  void ProtocolHandler::SendInitialResponse() {}

  ProcessResult ProtocolHandler::Process(
      UNUSED_ATTRIBUTE Buffer& rbuf,
      UNUSED_ATTRIBUTE const size_t thread_id) {
    return ProcessResult::TERMINATE;
  }

  void ProtocolHandler::Reset() {
    SetFlushFlag(false);
    responses.clear();
    request.Reset();
  }
  
  void ProtocolHandler::GetResult() {}
}  // namespace network
}  // namespace peloton

