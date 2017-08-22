//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// packet_manager.h
//
// Identification: src/include/network/packet_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "include/traffic_cop/traffic_cop.h"
#include "marshal.h"
#include "type/types.h"
// Packet content macros

namespace peloton {

namespace network {

typedef std::vector<std::unique_ptr<OutputPacket>> ResponseBuffer;

class ProtocolHandler {
 public:

  ProtocolHandler(tcop::TrafficCop *traffic_cop);

  virtual ~ProtocolHandler();

  /* Main switch case wrapper to process every packet apart from the startup
   * packet. Avoid flushing the response for extended protocols. */
  virtual ProcessPacketResult ProcessPacket(InputPacket* pkt, const size_t thread_id);

  /* Manage the startup packet */
  //  bool ManageStartupPacket();
  virtual void SendInitialResponse();

  virtual void Reset();

  virtual void GetResult();

  // Should we send the buffered packets right away?
  bool force_flush = false;

  // TODO declare a response buffer pool so that we can reuse the responses
  // so that we don't have to new packet each time
  ResponseBuffer responses;


  // The traffic cop used for this connection
  tcop::TrafficCop* traffic_cop_;

};

}  // namespace network
}  // namespace peloton
