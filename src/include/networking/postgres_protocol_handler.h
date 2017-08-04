//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_protocol_handler.h
//
// Identification: src/include/networking/postgres_protocol_handler.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network_manager.h"

#pragma once
namespace peloton {
namespace networking {

class PostgresProtocolHandler : public ProtocolHandler {
 public:
  inline PostgresProtocolHandler() {
    initial_read_size_ = sizeof(int32_t);
  }

  PacketReadState GetPacketFromBuffer(Buffer& rbuf, std::unique_ptr<InputPacket>& rpkt);

 protected:
  void GetSizeFromPktHeader(size_t start_index, Buffer& rbuf);

  inline bool IsReadDataAvailable(size_t bytes, Buffer& rbuf) {
    return ((rbuf.buf_ptr - 1) + bytes < rbuf.buf_size);
  }

  // The function tries to do a preliminary read to fetch the size value and
  // then reads the rest of the packet.
  // Assume: Packet length field is always 32-bit int
  bool ReadPacketHeader(Buffer& rbuf);

  // Tries to read the contents of a single packet, returns true on success, false
  // on failure.
  bool ReadPacket(Buffer& rbuf);

  virtual bool IsEndPacket();

  bool IsEndPacketSuppliment();

};

class PostgresJDBCProtocolHandler : public PostgresProtocolHandler{
  inline bool IsEndPacket() {
    return (IsEndPacketSuppliment() ||
            (rpkt_->msg_type == NetworkMessageType::SYNC_COMMAND));
  }
};

class PostgresSQLProtocolHandler : public PostgresProtcolHandler {
  inline bool IsEndPacket() {
    return (IsEndPacketSuppliment());
  }
};

class ProtolHandlerFactory {
 public:
  enum ProtocolHandlerType {
    PostgresJDBC,
    PostgresSQL
  };

  static std::unique_ptr<ProtocolHandler> CreateProtocolHandler(ProtocolHandler type) {
    switch (type) {
      case PostgresJDBC:
        return std::make_unique<PostgresJDBCProtocolHandler>();
      case PostgresSQL:
        return std::make_unique<PostgresSQLProtocolHandler>();
    }
  }

};

}
}
