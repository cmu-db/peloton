//
// Created by tim on 25/07/17.
//

#include "network_socket.h"

#pragma once
namespace peloton {
namespace networking {

/**
 * The PacketManager Interface
 */
class PacketManager {
 public:
  enum PacketManagerResultType {
    PacketDone,
    PacketNotDone,
    PacketEnd
  };

  virtual PacketManagerResultType GetPacketFromBuffer(Buffer&, std::unique_ptr<InputPacket>&);

 protected:
  static size_t initial_read_size_;
  std::unique_ptr<InputPacket> rpkt_ = nullptr;
};

class PostgresPacketManager : public PacketManager {
 public:
  inline PostgresPacketManager() {
    initial_read_size_ = sizeof(int32_t);
  }

  PacketManagerResultType
  GetPacketFromBuffer(Buffer& rbuf, std::unique_ptr<InputPacket>& rpkt);

 protected:
  void GetSizeFromPktHeader(size_t start_index, Buffer& rbuf);

  inline bool IsReadDataAvailable(size_t bytes, Buffer& rbuf) {
    return ((rbuf.buf_ptr - 1) + bytes < rbuf.buf_size);
  }

  bool ReadPacketHeader(Buffer& rbuf);

  bool ReadPacket(Buffer& rbuf);

  virtual bool IsEndPacket();

  bool IsEndPacketSuppliment();

};

class PostgresJDBCPacketManager : public PostgresPacketManager{
  inline bool IsEndPacket() {
    return (IsEndPacketSuppliment() ||
            (rpkt_->msg_type == NetworkMessageType::SYNC_COMMAND));
  }
};

class PostgresSQLPacketManager : public PostgresPacketManager{
  inline bool IsEndPacket() {
    return (IsEndPacketSuppliment());
  }
};

class PacketManagerFactory {
 public:
  enum PacketManagerType {
    PostgresJDBC,
    PostgresSQL
  };

  static std::unique_ptr<PacketManager> CreatePacketManager(PacketManagerType type) {
    switch (type) {
      case PostgresJDBC:
        return std::make_unique<PostgresJDBCPacketManager>();
      case PostgresSQL:
        return std::make_unique<PostgresSQLPacketManager>();
    }
  }

};

}
}
