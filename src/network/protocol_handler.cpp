//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// protocol_handler.cpp
//
// Identification: src/network/protocol_handler.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/protocol_handler.h"

#include <boost/algorithm/string.hpp>

namespace peloton {
namespace network {

ProtocolHandler::ProtocolHandler(tcop::TrafficCop *traffic_cop) {
  this->traffic_cop_ = traffic_cop;
}

ProtocolHandler::~ProtocolHandler() {}

ProcessResult ProtocolHandler::Process(UNUSED_ATTRIBUTE Buffer &rbuf,
                                       UNUSED_ATTRIBUTE const size_t
                                           thread_id) {
  return ProcessResult::TERMINATE;
}

void ProtocolHandler::Reset() {
  SetFlushFlag(false);
  responses_.clear();
  request_.Reset();
}

void ProtocolHandler::GetResult() {}
}  // namespace network
}  // namespace peloton
