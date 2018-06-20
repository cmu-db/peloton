//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_wire_protocol.h
//
// Identification: src/include/network/postgres_wire_protocol.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include "common/logger.h"
#include "network/wire_protocol.h"
#include "network/postgres_network_commands.h"

#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) ((x) >> 16)

namespace peloton {
namespace network {

class PostgresWireProtocol : public WireProtocol {
 public:
  Transition Process(ReadBuffer &in,
                     WriteBuffer &out,
                     size_t thread_id) override;
 private:
  bool startup_ = true;
  PostgresRawInputPacket curr_input_packet_;

  PostgresNetworkCommand PacketToCommand() {
    if (startup_) {
      int32_t proto_version = curr_input_packet_.GetInt(4);
      LOG_INFO("protocol version: %d", proto_version);
      if (proto_version == SSL_MESSAGE_VERNO) {
        return SslInitCommand(curr_input_packet_);
      }
    }
    curr_input_packet_.Clear();
  }

  bool BuildPacket(ReadBuffer &in) {
    if (!ReadPacketHeader(in)) return false;
    if (!in.HasMore(curr_input_packet_.len_)) return false;
    in.Read(curr_input_packet_.len_,
            curr_input_packet_.packet_head_,
            curr_input_packet_.packet_tail_);
    return true;
  }

  bool ReadPacketHeader(ReadBuffer &in) {
    if (curr_input_packet_.header_parsed_) return true;
    // Header format: 1 byte message type (only if non-startup)
    //              + 4 byte message size (inclusive of these 4 bytes)
    size_t header_size = startup_ ? sizeof(int32_t) : 1 + sizeof(int32_t);
    // Make sure the entire header is readable
    if (!in.HasMore(header_size)) return false;

    // The header is ready to be read, fill in fields accordingly
    if (!startup_)
      curr_input_packet_.msg_type_ = in.ReadValue<NetworkMessageType>();
    curr_input_packet_.len_ = ntohl(in.ReadValue<uint32_t>()) - sizeof(int32_t);
    // Extend the buffer as needed
    if (curr_input_packet_.len_ > in.Capacity()) {
      LOG_INFO("Extended Buffer size required for packet of size %ld",
               curr_input_packet_.len_);
      in.ExpandTo(curr_input_packet_.len_);
    }
    curr_input_packet_.header_parsed_ = true;
    return true;
  }
};
} // namespace peloton
} // namespace network