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
#include <utility>
#include "common/logger.h"
#include "network/protocol_interpreter.h"
#include "network/postgres_network_commands.h"
#include "network/netork_io_utils.h"

#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) ((x) >> 16)
#define MAKE_COMMAND(type)                                 \
  std::static_pointer_cast<PostgresNetworkCommand, type>(  \
      std::make_shared<type>(curr_input_packet_))

namespace peloton {
namespace network {

class PostgresProtocolInterpreter : public ProtocolInterpreter {
 public:
  inline Transition Process(std::shared_ptr<ReadBuffer> &in,
                            WriteQueue &out,
                            size_t thread_id) override {
    if (!BuildPacket(in)) return Transition::NEED_READ;
    std::shared_ptr<PostgresNetworkCommand> command = PacketToCommand();
    curr_input_packet_.Clear();
    return command->Exec(*this, out, thread_id);
  }

  inline void AddCommandLineOption(const std::string &name, std::string val) {
    cmdline_options_[name] = std::move(val);
  }

  inline void FinishStartup() { startup_ = false; }

  std::shared_ptr<PostgresNetworkCommand> PacketToCommand() {
//    if (startup_) return MAKE_COMMAND(StartupCommand);
    switch (curr_input_packet_.msg_type_) {
//      case NetworkMessageType::SIMPLE_QUERY_COMMAND:
//        return MAKE_COMMAND(SimpleQueryCommand);
//      case NetworkMessageType::PARSE_COMMAND:
//        return MAKE_COMMAND(ParseCommand);
//      case NetworkMessageType::BIND_COMMAND:
//        return MAKE_COMMAND(BindCommand);
//      case NetworkMessageType::DESCRIBE_COMMAND:
//        return MAKE_COMMAND(DescribeCommand);
//      case NetworkMessageType::EXECUTE_COMMAND:
//        return MAKE_COMMAND(ExecuteCommand);
//      case NetworkMessageType::SYNC_COMMAND:
//        return MAKE_COMMAND(SyncCommand);
//      case NetworkMessageType::CLOSE_COMMAND:
//        return MAKE_COMMAND(CloseCommand);
//      case NetworkMessageType::TERMINATE_COMMAND:
//        return MAKE_COMMAND(TerminateCommand);
//      case NetworkMessageType::NULL_COMMAND:
//        return MAKE_COMMAND(NullCommand);
      default:
        throw NetworkProcessException("Unexpected Packet Type: " +
            std::to_string(static_cast<int>(curr_input_packet_.msg_type_)));
    }
  }

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
    curr_input_packet_.len_ = in->ReadInt<uint32_t>() - sizeof(uint32_t);

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

/**
 * Wrapper around an I/O layer WriteQueue to provide Postgres-sprcific
 * helper methods.
 */
class PostgresPacketWriter {
 public:
  /*
   * Instantiates a new PostgresPacketWriter backed by the given WriteQueue
   */
  PostgresPacketWriter(WriteQueue &write_queue) : queue_(write_queue) {}

  ~PostgresPacketWriter() {
    // Make sure no packet is being written on destruction, otherwise we are
    // malformed write buffer
    PELOTON_ASSERT(curr_packet_len_ == nullptr);
  }

  /**
   * Write out a packet with a single byte (e.g. SSL_YES or SSL_NO). This is a
   * special case since no size field is provided.
   * @param type Type of message to write out
   */
  inline void WriteSingleBytePacket(NetworkMessageType type) {
    // Make sure no active packet being constructed
    PELOTON_ASSERT(curr_packet_len_ == nullptr);
    queue_.BufferWriteRawValue(type);
  }

  /**
   * Begin writing a new packet. Caller can use other append methods to write
   * contents to the packet. An explicit call to end packet must be made to
   * make these writes valid.
   * @param type
   * @return self-reference for chaining
   */
  PostgresPacketWriter &BeginPacket(NetworkMessageType type) {
    // No active packet being constructed
    PELOTON_ASSERT(curr_packet_len_ == nullptr);
    queue_.BufferWriteRawValue(type);
    // Remember the size field since we will need to modify it as we go along.
    // It is important that our size field is contiguous and not broken between
    // two buffers.
    queue_.BufferWriteRawValue<int32_t>(0, false);
    WriteBuffer &tail = *(queue_.buffers_[queue_.buffers_.size() - 1]);
    curr_packet_len_ =
        reinterpret_cast<uint32_t *>(&tail.buf_[tail.size_ - sizeof(int32_t)]);
    return *this;
  }

  /**
   * Append raw bytes from specified memory location into the write queue.
   * There must be a packet active in the writer.
   * @param src memory location to write from
   * @param len number of bytes to write
   * @return self-reference for chaining
   */
  inline PostgresPacketWriter &AppendRaw(const void *src, size_t len) {
    PELOTON_ASSERT(curr_packet_len_ != nullptr);
    queue_.BufferWriteRaw(src, len);
    // Add the size field to the len of the packet. Be mindful of byte
    // ordering. We switch to network ordering only when the packet is finished
    *curr_packet_len_ += len;
    return *this;
  }

  /**
   * Append a value onto the write queue. There must be a packet active in the
   * writer. No byte order conversion is performed. It is up to the caller to
   * do so if needed.
   * @tparam T type of value to write
   * @param val value to write
   * @return self-reference for chaining
   */
  template<typename T>
  inline PostgresPacketWriter &AppendRawValue(T val) {
    return AppendRaw(&val, sizeof(T));
  }

  /**
   * Append an integer of specified length onto the write queue. (1, 2, 4, or 8
   * bytes). It is assumed that these bytes need to be converted to network
   * byte ordering.
   * @tparam T type of value to read off. Has to be size 1, 2, 4, or 8.
   * @param val value to write
   * @return self-reference for chaining
   */
  template <typename T>
  PostgresPacketWriter &AppendInt(T val) {
    // We only want to allow for certain type sizes to be used
    // After the static assert, the compiler should be smart enough to throw
    // away the other cases and only leave the relevant return statement.
    static_assert(sizeof(T) == 1
                      || sizeof(T) == 2
                      || sizeof(T) == 4
                      || sizeof(T) == 8, "Invalid size for integer");
    switch (sizeof(T)) {
      case 1: return AppendRawValue<T>(val);
      case 2: return AppendRawValue<T>(ntohs(val));
      case 4: return AppendRawValue<T>(ntohl(val));
      case 8: return AppendRawValue<T>(ntohll(val));
      // Will never be here due to compiler optimization
      default: throw NetworkProcessException("");
    }
  }

  /**
   * Append a string onto the write queue.
   * @param str the string to append
   * @param nul_terminate whether the nul terminaor should be written as well
   * @return self-reference for chaining
   */
  inline PostgresPacketWriter &AppendString(const std::string &str,
                                            bool nul_terminate = true) {
    return AppendRaw(str.data(), nul_terminate ? str.size() + 1 : str.size());
  }

  /**
   * End the packet. A packet write must be in progress and said write is not
   * well-formed until this method is called.
   */
  inline void EndPacket() {
    PELOTON_ASSERT(curr_packet_len_ != nullptr);
    // Switch to network byte ordering, add the 4 bytes of size field
    *curr_packet_len_ = htonl(*curr_packet_len_ + sizeof(int32_t));
    curr_packet_len_ = nullptr;
  }
 private:
  // We need to keep track of the size field of the current packet,
  // so we can update it as more bytes are written into this packet.
  uint32_t *curr_packet_len_ = nullptr;
  // Underlying WriteQueue backing this writer
  WriteQueue &queue_;
};

class PostgresWireUtilities {
 public:
  PostgresWireUtilities() = delete;

  static inline void SendErrorResponse(
      PostgresPacketWriter &writer,
      std::vector<std::pair<NetworkMessageType, std::string>> error_status) {
    writer.BeginPacket(NetworkMessageType::ERROR_RESPONSE);
    for (const auto &entry : error_status) {
      writer.AppendRawValue(entry.first);
      writer.AppendString(entry.second);
    }
    // Nul-terminate packet
    writer.AppendRawValue<uchar>(0)
          .EndPacket();
  }

  static inline void SendStartupResponse(PostgresPacketWriter &writer) {
    // auth-ok
    writer.BeginPacket(NetworkMessageType::AUTHENTICATION_REQUEST).EndPacket();

    // parameter status map
    for (auto &entry : parameter_status_map_)
      writer.BeginPacket(NetworkMessageType::PARAMETER_STATUS)
            .AppendString(entry.first)
            .AppendString(entry.second)
            .EndPacket();

    // ready-for-query
    SendReadyForQuery(writer, NetworkTransactionStateType::IDLE);
  }

  static inline void SendReadyForQuery(PostgresPacketWriter &writer,
                                       NetworkTransactionStateType txn_status) {
    writer.BeginPacket(NetworkMessageType::READY_FOR_QUERY)
        .AppendRawValue(txn_status)
        .EndPacket();
  }

  static inline void SendEmptyQueryResponse(PostgresPacketWriter &writer) {
    writer.BeginPacket(NetworkMessageType::EMPTY_QUERY_RESPONSE).EndPacket();
  }

  static inline void SendCommandCompleteResponse(PostgresPacketWriter &writer,
                                                 const QueryType &query_type,
                                                 int rows) {
    std::string tag = QueryTypeToString(query_type);
    switch (query_type) {
      case QueryType::QUERY_INSERT:tag += " 0 " + std::to_string(rows);
        break;
      case QueryType::QUERY_BEGIN:
      case QueryType::QUERY_COMMIT:
      case QueryType::QUERY_ROLLBACK:
      case QueryType::QUERY_CREATE_TABLE:
      case QueryType::QUERY_CREATE_DB:
      case QueryType::QUERY_CREATE_INDEX:
      case QueryType::QUERY_CREATE_TRIGGER:
      case QueryType::QUERY_PREPARE:break;
      default:tag += " " + std::to_string(rows);
    }
    writer.BeginPacket(NetworkMessageType::COMMAND_COMPLETE)
          .AppendString(tag)
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