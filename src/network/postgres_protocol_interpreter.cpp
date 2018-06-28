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
#include "network/postgres_protocol_interpreter.h"

#define MAKE_COMMAND(type)                                 \
  std::static_pointer_cast<PostgresNetworkCommand, type>(  \
      std::make_shared<type>(std::move(curr_input_packet_.buf_)))

namespace peloton {
namespace network {
Transition PostgresProtocolInterpreter::Process(std::shared_ptr<ReadBuffer> in,
                                                std::shared_ptr<WriteQueue> out,
                                                CallbackFunc callback) {
  if (!TryBuildPacket(in)) return Transition::NEED_READ;
  std::shared_ptr<PostgresNetworkCommand> command = PacketToCommand();
  curr_input_packet_.Clear();
  PostgresPacketWriter writer(*out);
  if (command->FlushOnComplete()) out->ForceFlush();
  return command->Exec(*this, writer, callback);
}

bool PostgresProtocolInterpreter::TryBuildPacket(std::shared_ptr<ReadBuffer> &in) {
  if (!TryReadPacketHeader(in)) return false;

  size_t size_needed = curr_input_packet_.extended_
                       ? curr_input_packet_.len_
                           - curr_input_packet_.buf_->BytesAvailable()
                       : curr_input_packet_.len_;
  if (!in->HasMore(size_needed)) return false;

  // copy bytes only if the packet is longer than the read buffer,
  // otherwise we can use the read buffer to save space
  if (curr_input_packet_.extended_)
    curr_input_packet_.buf_->FillBufferFrom(*in, size_needed);
  return true;
}

bool PostgresProtocolInterpreter::TryReadPacketHeader(std::shared_ptr<ReadBuffer> &in) {
  if (curr_input_packet_.header_parsed_) return true;

  // Header format: 1 byte message type (only if non-startup)
  //              + 4 byte message size (inclusive of these 4 bytes)
  size_t header_size = startup_ ? sizeof(int32_t) : 1 + sizeof(int32_t);
  // Make sure the entire header is readable
  if (!in->HasMore(header_size)) return false;

  // The header is ready to be read, fill in fields accordingly
  if (!startup_)
    curr_input_packet_.msg_type_ = in->ReadRawValue<NetworkMessageType>();
  curr_input_packet_.len_ = in->ReadValue<uint32_t>() - sizeof(uint32_t);

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

std::shared_ptr<PostgresNetworkCommand> PostgresProtocolInterpreter::PacketToCommand() {
  if (startup_) return MAKE_COMMAND(StartupCommand);
  switch (curr_input_packet_.msg_type_) {
    case NetworkMessageType::SIMPLE_QUERY_COMMAND:
      return MAKE_COMMAND(SimpleQueryCommand);
    case NetworkMessageType::PARSE_COMMAND:
      return MAKE_COMMAND(ParseCommand);
    case NetworkMessageType::BIND_COMMAND
      :return MAKE_COMMAND(BindCommand);
    case NetworkMessageType::DESCRIBE_COMMAND:
      return MAKE_COMMAND(DescribeCommand);
    case NetworkMessageType::EXECUTE_COMMAND:
      return MAKE_COMMAND(ExecuteCommand);
    case NetworkMessageType::SYNC_COMMAND
      :return MAKE_COMMAND(SyncCommand);
    case NetworkMessageType::CLOSE_COMMAND:
      return MAKE_COMMAND(CloseCommand);
    case NetworkMessageType::TERMINATE_COMMAND:
      return MAKE_COMMAND(TerminateCommand);
    default:
      throw NetworkProcessException("Unexpected Packet Type: " +
          std::to_string(static_cast<int>(curr_input_packet_.msg_type_)));
  }
}

} // namespace network
} // namespace peloton