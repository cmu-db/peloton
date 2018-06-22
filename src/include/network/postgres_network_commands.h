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

#define DEFINE_COMMAND(name)                                                        \
class name : public PostgresNetworkCommand {                                        \
 public:                                                                            \
  explicit name(PostgresInputPacket &input_packet)                                  \
     : PostgresNetworkCommand(std::move(input_packet)) {}                           \
  virtual Transition Exec(PostgresProtocolInterpreter &, WriteQueue &, size_t) override {}   \
}

namespace peloton {
namespace network {

class PostgresProtocolInterpreter;

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
  virtual Transition Exec(PostgresProtocolInterpreter &protocol_obj,
                          WriteQueue &out,
                          size_t thread_id) = 0;
 protected:
  PostgresNetworkCommand(PostgresInputPacket input_packet)
      : input_packet_(input_packet) {}

  PostgresInputPacket input_packet_;
};

//DEFINE_COMMAND(StartupCommand);
//DEFINE_COMMAND(SimpleQueryCommand);
//DEFINE_COMMAND(ParseCommand);
//DEFINE_COMMAND(BindCommand);
//DEFINE_COMMAND(DescribeCommand);
//DEFINE_COMMAND(ExecuteCommand);
//DEFINE_COMMAND(SyncCommand);
//DEFINE_COMMAND(CloseCommand);
//DEFINE_COMMAND(TerminateCommand);
//DEFINE_COMMAND(NullCommand);

} // namespace network
} // namespace peloton
