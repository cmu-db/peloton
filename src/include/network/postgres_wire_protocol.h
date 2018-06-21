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
#define MAKE_COMMAND(type)                                 \
  std::static_pointer_cast<PostgresNetworkCommand, type>(  \
      std::make_shared<type>(curr_input_packet_))

namespace peloton {
namespace network {
class PostgresWireProtocol : public WireProtocol {
 public:
  // TODO(Tianyu): Remove tcop when tcop refactor complete
  PostgresWireProtocol(tcop::TrafficCop *tcop) : tcop_(tcop) {}

  inline Transition Process(std::shared_ptr<ReadBuffer> &in,
                            WriteQueue &out,
                            size_t thread_id) override {
    if (!BuildPacket(in)) return Transition::NEED_READ;
    std::shared_ptr<PostgresNetworkCommand> command = PacketToCommand();
    curr_input_packet_.Clear();
    return command->Exec(*this, out, thread_id);
  }

  inline void AddCommandLineOption(std::string name, std::string val) {
    cmdline_options_[name] = val;
  }

  inline void FinishStartup() { startup_ = false; }

  std::shared_ptr<PostgresNetworkCommand> PacketToCommand() {
    if (startup_) return MAKE_COMMAND(StartupCommand);
    switch (curr_input_packet_.msg_type_) {
      case NetworkMessageType::SIMPLE_QUERY_COMMAND:
        return MAKE_COMMAND(SimpleQueryCommand);
      case NetworkMessageType::PARSE_COMMAND:return MAKE_COMMAND(ParseCommand);
      case NetworkMessageType::BIND_COMMAND:return MAKE_COMMAND(BindCommand);
      case NetworkMessageType::DESCRIBE_COMMAND:
        return MAKE_COMMAND(DescribeCommand);
      case NetworkMessageType::EXECUTE_COMMAND:
        return MAKE_COMMAND(ExecuteCommand);
      case NetworkMessageType::SYNC_COMMAND:return MAKE_COMMAND(SyncCommand);
      case NetworkMessageType::CLOSE_COMMAND:return MAKE_COMMAND(CloseCommand);
      case NetworkMessageType::TERMINATE_COMMAND:
        return MAKE_COMMAND(TerminateCommand);
      case NetworkMessageType::NULL_COMMAND:return MAKE_COMMAND(NullCommand);
      default:
        throw NetworkProcessException("Unexpected Packet Type: " +
            std::to_string(static_cast<int>(curr_input_packet_.msg_type_)));
    }
  }
  // TODO(Tianyu): Remove this when tcop refactor complete
  tcop::TrafficCop *tcop_;
 private:
  bool startup_ = true;
  PostgresInputPacket curr_input_packet_{};
  std::unordered_map<std::string, std::string> cmdline_options_;

  bool BuildPacket(std::shared_ptr<ReadBuffer> &in) {
    if (!ReadPacketHeader(in)) return false;

    size_t size_needed = curr_input_packet_.extended_
                         ? curr_input_packet_.len_
                             - curr_input_packet_.buf_->BytesAvailable()
                         : curr_input_packet_.len_;
    if (!in->HasMore(size_needed)) return false;

    if (curr_input_packet_.extended_)
      curr_input_packet_.buf_->FillBufferFrom(*in, size_needed);
    return true;
  }

  bool ReadPacketHeader(std::shared_ptr<ReadBuffer> &in) {
    if (curr_input_packet_.header_parsed_) return true;

    // Header format: 1 byte message type (only if non-startup)
    //              + 4 byte message size (inclusive of these 4 bytes)
    size_t header_size = startup_ ? sizeof(int32_t) : 1 + sizeof(int32_t);
    // Make sure the entire header is readable
    if (!in->HasMore(header_size)) return false;

    // The header is ready to be read, fill in fields accordingly
    if (!startup_)
      curr_input_packet_.msg_type_ = in->ReadRawValue<NetworkMessageType>();
    curr_input_packet_.len_ = in->ReadInt(sizeof(int32_t)) - sizeof(int32_t);

    // Extend the buffer as needed
    if (curr_input_packet_.len_ > in->Capacity()) {
      LOG_INFO("Extended Buffer size required for packet of size %ld",
               curr_input_packet_.len_);
      // Allocate a larger buffer and copy bytes off from the I/O layer's buffer
      curr_input_packet_.buf_ =
          std::make_shared<ReadBuffer>(curr_input_packet_.len_);
      curr_input_packet_.extended_ = true;
    } else {
      curr_input_packet_.buf_ = in;
    }

    curr_input_packet_.header_parsed_ = true;
    return true;
  }
};

class PostgresWireUtilities {
 public:
  PostgresWireUtilities() = delete;

  static inline void SendErrorResponse(
      WriteQueue &out,
      std::vector<std::pair<NetworkMessageType, std::string>> error_status) {
    out.BeginPacket(NetworkMessageType::ERROR_RESPONSE);
    for (const auto &entry : error_status) {
      out.AppendRawValue(entry.first);
      out.AppendString(entry.second);
    }
    // Nul-terminate packet
    out.AppendRawValue<uchar>(0)
        .EndPacket();
  }

  static inline void SendStartupResponse(WriteQueue &out) {
    // auth-ok
    out.BeginPacket(NetworkMessageType::AUTHENTICATION_REQUEST).EndPacket();

    // parameter status map
    for (auto &entry : parameter_status_map_)
      out.BeginPacket(NetworkMessageType::PARAMETER_STATUS)
          .AppendString(entry.first)
          .AppendString(entry.second)
          .EndPacket();

    // ready-for-query
    SendReadyForQuery(NetworkTransactionStateType::IDLE, out);
  }

  static inline void SendReadyForQuery(NetworkTransactionStateType txn_status,
                                       WriteQueue &out) {
    out.BeginPacket(NetworkMessageType::READY_FOR_QUERY)
        .AppendRawValue(txn_status)
        .EndPacket();
  }

 private:
  // TODO(Tianyu): It looks broken that this never changes.
  // TODO(Tianyu): Also, Initialize.
  static const std::unordered_map<std::string, std::string>
      parameter_status_map_;
};
} // namespace network
} // namespace peloton