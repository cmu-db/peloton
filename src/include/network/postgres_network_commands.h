//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_network_commands.h
//
// Identification: src/include/network/postgres_network_commands.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "network/network_types.h"
#include "network/postgres_protocol_handler.h"
#include "traffic_cop/traffic_cop.h"
#include "network/marshal.h"

#define DEFINE_COMMAND(name, protocol_type)                                    \
class name : public PostgresNetworkCommand {                                   \
 public:                                                                       \
  name(PostgresRawInputPacket &input_packet)                                   \
     : PostgresNetworkCommand(std::move(input_packet), protocol_type) {}       \
  virtual Transition Exec(PostgresProtocolHandler &handler) override;          \
}

namespace peloton {
namespace network {

struct PostgresRawInputPacket {
  NetworkMessageType msg_type_ = NetworkMessageType::NULL_COMMAND;
  size_t len_ = 0;
  ByteBuf::const_iterator packet_head_, packet_tail_;
  bool header_parsed_ = false;

  PostgresRawInputPacket() = default;
  PostgresRawInputPacket(const PostgresRawInputPacket &) = default;
  PostgresRawInputPacket(PostgresRawInputPacket &&) = default;

  inline void Clear() {
    msg_type_ = NetworkMessageType::NULL_COMMAND;
    len_ = 0;
    header_parsed_ = false;
  }

  int GetInt(uint8_t len) {
    int value = 0;
    std::copy(packet_head_, packet_head_ + len,
              reinterpret_cast<uchar *>(&value));
    switch (len) {
      case 1:break;
      case 2:value = ntohs(value);
        break;
      case 4:value = ntohl(value);
        break;
      default:
        throw NetworkProcessException(
            "Error when de-serializing: Invalid int size");
    }
    packet_head_ += len;
    return value;
  }

  inline uchar GetByte() { return (uchar) GetInt(1); }

  inline void GetBytes(size_t len, ByteBuf &result) {
    result.insert(std::end(result), packet_head_, packet_head_ + len);
    packet_head_ += len;
  }

  std::string GetString(size_t len) {
    // TODO(Tianyu): This looks broken, some broken-looking code depends on it
    // though
    if (len == 0) return "";
    // Nul character at end
    auto result = std::string(packet_head_, packet_head_ + (len - 1));
    packet_head_ += len;
    return result;
  }

  std::string GetString() {
    // Find nul-terminator
    auto find_itr = std::find(packet_head_, packet_tail_, 0);
    if (find_itr == packet_tail_)
      throw NetworkProcessException("Expected nil at end of packet, none found");
    auto result = std::string(packet_head_, find_itr);
    packet_head_ = find_itr + 1;
    return result;
  }
};

class PostgresNetworkCommand {
 public:
  virtual Transition Exec(PostgresProtocolHandler &handler) = 0;
 protected:
  PostgresNetworkCommand(PostgresRawInputPacket input_packet,
                         ResponseProtocol response_protocol)
      : input_packet_(input_packet),
        response_protocol_(response_protocol) {}

  PostgresRawInputPacket input_packet_;
  const ResponseProtocol response_protocol_;
};

// TODO(Tianyu): Fix response types
DEFINE_COMMAND(SslInitCommand, ResponseProtocol::SIMPLE);
DEFINE_COMMAND(StartupCommand, ResponseProtocol::SIMPLE);
DEFINE_COMMAND(SimpleQueryCommand, ResponseProtocol::SIMPLE);
DEFINE_COMMAND(ParseCommand, ResponseProtocol::NO);
DEFINE_COMMAND(BindCommand, ResponseProtocol::NO);
DEFINE_COMMAND(DescribeCommand, ResponseProtocol::NO);
DEFINE_COMMAND(ExecuteCommand, ResponseProtocol::EXTENDED);
DEFINE_COMMAND(SyncCommand, ResponseProtocol::SIMPLE);
DEFINE_COMMAND(CloseCommand, ResponseProtocol::NO);
DEFINE_COMMAND(TerminateCommand, ResponseProtocol::NO);
DEFINE_COMMAND(NullCommand, ResponseProtocol::NO);

} // namespace network
} // namespace peloton
