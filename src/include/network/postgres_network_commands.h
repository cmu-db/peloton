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
#include "traffic_cop/traffic_cop.h"
#include "network/marshal.h"

#define DEFINE_COMMAND(name, protocol_type)                                         \
class name : public PostgresNetworkCommand {                                        \
 public:                                                                            \
  explicit name(PostgresRawInputPacket &input_packet)                               \
     : PostgresNetworkCommand(std::move(input_packet), protocol_type) {}            \
  virtual Transition Exec(PostgresWireProtocol &, WriteQueue &, size_t) override;  \
}

namespace peloton {
namespace network {

class PostgresWireProtocol;

struct PostgresInputPacket {
  NetworkMessageType msg_type_ = NetworkMessageType::NULL_COMMAND;
  size_t len_ = 0;
  std::shared_ptr<ReadBuffer> buf_;
  bool header_parsed_ = false, extended_ = false;

  PostgresInputPacket() = default;
  PostgresInputPacket(const PostgresInputPacket &) = default;
  PostgresInputPacket(PostgresInputPacket &&) = default;

  inline void Clear() {
    msg_type_ = NetworkMessageType::NULL_COMMAND;
    len_ = 0;
    buf_ = nullptr;
    header_parsed_ = false;
  }
};

class PostgresNetworkCommand {
 public:
  virtual Transition Exec(PostgresWireProtocol &protocol_obj,
                          WriteQueue &out,
                          size_t thread_id) = 0;
 protected:
  PostgresNetworkCommand(PostgresInputPacket input_packet,
                         ResponseProtocol response_protocol)
      : input_packet_(input_packet),
        response_protocol_(response_protocol) {}

  PostgresInputPacket input_packet_;
  const ResponseProtocol response_protocol_;
};

// TODO(Tianyu): Fix response types
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
