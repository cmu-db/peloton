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

namespace peloton {
namespace network {

class PostgresProtocolInterpreter : public ProtocolInterpreter {
 public:
  // TODO(Tianyu): Is this even the right thread id? It seems that all the
  // concurrency code is dependent on this number.
  explicit PostgresProtocolInterpreter(size_t thread_id) = default;

  Transition Process(std::shared_ptr<ReadBuffer> in,
                     std::shared_ptr<WriteQueue> out,
                     callback_func callback) override;

  inline void GetResult() override {}

  inline void AddCmdlineOption(const std::string &key, std::string value) {
    cmdline_options_[key] = std::move(value);
  }

  inline void FinishStartup() { startup_ = false; }

  inline tcop::ClientProcessState &ClientProcessState() { return state_; }

  // TODO(Tianyu): WTF is this?
  void ExecQueryMessageGetResult(ResultType status);
  void ExecExecuteMessageGetResult(ResultType status);

  std::vector<int> result_format_;
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