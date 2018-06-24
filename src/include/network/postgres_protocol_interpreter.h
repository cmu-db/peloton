//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_protocol_interpreter.h
//
// Identification: src/include/network/postgres_wire_protocol.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include <utility>
#include "common/logger.h"
#include "network/protocol_interpreter.h"
#include "network/postgres_network_commands.h"
#include "traffic_cop/tcop.h"

#define MAKE_COMMAND(type)                                 \
  std::static_pointer_cast<PostgresNetworkCommand, type>(  \
      std::make_shared<type>(std::move(curr_input_packet_.buf_)))

namespace peloton {
namespace network {

class PostgresProtocolInterpreter : public ProtocolInterpreter {
 public:
  // TODO(Tianyu): Is this even the right thread id? It seems that all the
  // concurrency code is dependent on this number.
  PostgresProtocolInterpreter(size_t thread_id) = default;

  Transition Process(std::shared_ptr<ReadBuffer> in,
                     std::shared_ptr<WriteQueue> out,
                     callback_func callback) override;

  inline void GetResult() override {}

  inline void AddCmdlineOption(const std::string &key, std::string value) {
    cmdline_options_[key] = std::move(value);
  }

  inline void FinishStartup() { startup_ = false; }

  inline tcop::ClientProcessState &ClientProcessState() { return state_; }

 private:
  bool startup_ = true;
  PostgresInputPacket curr_input_packet_{};
  std::unordered_map<std::string, std::string> cmdline_options_;
  tcop::ClientProcessState state_;

  bool TryBuildPacket(std::shared_ptr<ReadBuffer> &in);
  bool TryReadPacketHeader(std::shared_ptr<ReadBuffer> &in);
  std::shared_ptr<PostgresNetworkCommand> PacketToCommand();
};

} // namespace network
} // namespace peloton