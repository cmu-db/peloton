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

#pragma once

#include "traffic_cop/traffic_cop.h"
#include "marshal.h"
#include "common/internal_types.h"
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

  /* Manage the startup packet */
  //  bool ManageStartupPacket();
  virtual void SendInitialResponse();

  virtual bool ProcessInitialPacket(InputPacket* pkt, Client client, bool ssl_able, bool& ssl_sent, bool& finish_startup_packet) = 0;

  virtual ProcessResult Process(Buffer &rbuf, const size_t thread_id);

  virtual void Reset();

  virtual void GetResult();

  void SetFlushFlag(bool flush) {force_flush_ = flush;}

  bool GetFlushFlag() {return force_flush_;}

  bool force_flush_ = false;

  // TODO declare a response buffer pool so that we can reuse the responses
  // so that we don't have to new packet each time
  ResponseBuffer responses;

  InputPacket request;                // Used for reading a single request

  // The traffic cop used for this connection
  tcop::TrafficCop* traffic_cop_;

};

}  // namespace network
}  // namespace peloton
