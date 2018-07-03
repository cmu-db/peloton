//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_protocol_utils.h
//
// Identification: src/include/network/postgres_protocol_utils.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <boost/algorithm/string.hpp>
#include "network/network_io_utils.h"
#include "common/statement.h"

#define NULL_CONTENT_SIZE (-1)
namespace peloton {
namespace network {

// TODO(Tianyu): It looks very broken that this never changes.
// clang-format off
const std::unordered_map<std::string, std::string>
    parameter_status_map = {
        {"application_name", "psql"},
        {"client_encoding", "UTF8"},
        {"DateStyle", "ISO, MDY"},
        {"integer_datetimes", "on"},
        {"IntervalStyle", "postgres"},
        {"is_superuser", "on"},
        {"server_encoding", "UTF8"},
        {"server_version", "9.5devel"},
        {"session_authorization", "postgres"},
        {"standard_conforming_strings", "on"},
        {"TimeZone", "US/Eastern"}
    };
// clang-format on

/**
 * Encapsulates an input packet
 */
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
   * Write out a packet with a single type. Some messages will be
   * special cases since no size field is provided. (SSL_YES, SSL_NO)
   * @param type Type of message to write out
   */
  inline void WriteSingleTypePacket(NetworkMessageType type) {
    // Make sure no active packet being constructed
    PELOTON_ASSERT(curr_packet_len_ == nullptr);
    switch (type) {
      case NetworkMessageType::SSL_YES:
      case NetworkMessageType::SSL_NO:
        queue_.BufferWriteRawValue(type);
        break;
      default:
        BeginPacket(type).EndPacket();
    }
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
   * Append a value of specified length onto the write queue. (1, 2, 4, or 8
   * bytes). It is assumed that these bytes need to be converted to network
   * byte ordering.
   * @tparam T type of value to read off. Has to be size 1, 2, 4, or 8.
   * @param val value to write
   * @return self-reference for chaining
   */
  template<typename T>
  inline PostgresPacketWriter &AppendValue(T val) {
    // We only want to allow for certain type sizes to be used
    // After the static assert, the compiler should be smart enough to throw
    // away the other cases and only leave the relevant return statement.
    static_assert(sizeof(T) == 1
                      || sizeof(T) == 2
                      || sizeof(T) == 4
                      || sizeof(T) == 8, "Invalid size for integer");

    switch (sizeof(T)) {
      case 1: return AppendRawValue(val);
      case 2: return AppendRawValue(_CAST(T, htobe16(_CAST(uint16_t, val))));
      case 4: return AppendRawValue(_CAST(T, htobe32(_CAST(uint32_t, val))));
      case 8: return AppendRawValue(_CAST(T, htobe64(_CAST(uint64_t, val))));
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

  inline void WriteErrorResponse(
      std::vector<std::pair<NetworkMessageType, std::string>> error_status) {
    BeginPacket(NetworkMessageType::ERROR_RESPONSE);

    for (const auto &entry : error_status)
      AppendRawValue(entry.first)
          .AppendString(entry.second);

    // Nul-terminate packet
    AppendRawValue<uchar>(0)
        .EndPacket();
  }

  inline void WriteReadyForQuery(NetworkTransactionStateType txn_status) {
    BeginPacket(NetworkMessageType::READY_FOR_QUERY)
        .AppendRawValue(txn_status)
        .EndPacket();
  }

  inline void WriteStartupResponse() {
    BeginPacket(NetworkMessageType::AUTHENTICATION_REQUEST)
        .AppendValue<int32_t>(0)
        .EndPacket();

    for (auto &entry : parameter_status_map)
      BeginPacket(NetworkMessageType::PARAMETER_STATUS)
          .AppendString(entry.first)
          .AppendString(entry.second)
          .EndPacket();
    WriteReadyForQuery(NetworkTransactionStateType::IDLE);
  }

  inline void WriteEmptyQueryResponse() {
    BeginPacket(NetworkMessageType::EMPTY_QUERY_RESPONSE)
        .EndPacket();
  }

  inline void WriteTupleDescriptor(const std::vector<FieldInfo> &tuple_descriptor) {
    if (tuple_descriptor.empty()) return;
    BeginPacket(NetworkMessageType::ROW_DESCRIPTION);
    AppendValue<uint16_t>(tuple_descriptor.size());
    for (auto &col : tuple_descriptor) {
      AppendString(std::get<0>(col));
      // TODO: Table Oid (int32)
      AppendValue<int32_t>(0);
      // TODO: Attr id of column (int16)
      AppendValue<int16_t>(0);
      // Field data type (int32)
      AppendValue<int32_t>(std::get<1>(col));
      // Data type size (int16)
      AppendValue<int16_t>(std::get<2>(col));
      // Type modifier (int32)
      AppendValue<int32_t>(-1);
      AppendValue<int16_t >(0);
    }
    EndPacket();
  }

  inline void WriteDataRows(const std::vector<ResultValue> &results,
                            size_t num_columns) {
    if (results.empty() || num_columns == 0) return;
    size_t num_rows = results.size() / num_columns;
    for (size_t i = 0; i < num_rows; i++) {
      BeginPacket(NetworkMessageType::DATA_ROW)
          .AppendValue<uint16_t>(num_columns);
      for (size_t j = 0; j < num_columns; j++) {
        auto content = results[i * num_columns + j];
        if (content.empty())
          AppendValue<uint32_t>(NULL_CONTENT_SIZE);
        else
          AppendValue<uint32_t>(content.size())
              .AppendString(content, false);

      }
      EndPacket();
    }
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

} // namespace network
} // namespace peloton
