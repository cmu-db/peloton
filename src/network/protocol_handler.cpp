//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// protocol_handler.h
//
// Identification: src/include/network/protocol_handler.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "network/protocol_handler.h"

#include <boost/algorithm/string.hpp>

namespace peloton {
namespace network {

  ProtocolHandler::ProtocolHandler() {
    traffic_cop_.reset(new tcop::TrafficCop());
  }

  ProtocolHandler::~ProtocolHandler() {}

  bool ProtocolHandler::ProcessPacket(
      UNUSED_ATTRIBUTE InputPacket* pkt,
      UNUSED_ATTRIBUTE const size_t thread_id)
  {
    return false;
  }

  /* Manage the startup packet */
  //  bool ManageStartupPacket();
  void ProtocolHandler::SendInitialResponse() {}

  void ProtocolHandler::Reset() {}

}  // namespace network
}  // namespace peloton

