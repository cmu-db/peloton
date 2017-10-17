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

#include "include/traffic_cop/traffic_cop.h"
#include "marshal.h"
#include "type/types.h"
#include "logging/wal_log_manager.h"
// Packet content macros

namespace peloton {

namespace network {

typedef std::vector<std::unique_ptr<OutputPacket>> ResponseBuffer;

class ProtocolHandler {
 public:

  ProtocolHandler(tcop::TrafficCop *traffic_cop, logging::WalLogManager* log_manager);

  virtual ~ProtocolHandler();

  /* Main switch case wrapper to process every packet apart from the startup
   * packet. Avoid flushing the response for extended protocols. */

  /* Manage the startup packet */
  //  bool ManageStartupPacket();
  virtual void SendInitialResponse();

  virtual ProcessResult Process(Buffer &rbuf, const size_t thread_id);

  virtual void Reset();

  virtual void GetResult();

  // Should we send the buffered packets right away?
  bool force_flush = false;

  // TODO declare a response buffer pool so that we can reuse the responses
  // so that we don't have to new packet each time
  ResponseBuffer responses;

  InputPacket request;                // Used for reading a single request

  // The traffic cop used for this connection
  tcop::TrafficCop* traffic_cop_;
  logging::WalLogManager* log_manager_;

};

}  // namespace network
}  // namespace peloton
