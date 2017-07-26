//
// Created by tim on 25/07/17.
//

#include "network_socket.h"

#pragma once
namespace peloton {
namespace networking {

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

  bool ReadPacketHeader(Buffer& rbuf) {
    // get packet size from the header

    if (!IsReadDataAvailable(initial_read_size_, rbuf)) {
      rpkt_->header_parsed = false;
      return false;
    }

    rpkt_->msg_type =
        static_cast<NetworkMessageType>(rbuf.GetByte(rbuf.buf_ptr));

    GetSizeFromPktHeader(rbuf.buf_ptr + 1, rbuf);

    rpkt_->ReserveExtendedBuffer();

    // we have processed the data, move buffer pointer
    rbuf.buf_ptr += initial_read_size_;
    rpkt_->header_parsed = true;

    return true;
  }

  bool ReadPacket(Buffer& rbuf) {
    // extended packet mode
    auto bytes_available = rbuf.buf_size - rbuf.buf_ptr;
    auto bytes_required = rpkt_->ExtendedBytesRequired();
    // read minimum of the two ranges
    auto read_size = std::min(bytes_available, bytes_required);
    rpkt_->AppendToExtendedBuffer(rbuf.Begin() + rbuf.buf_ptr,
                                  rbuf.Begin() + rbuf.buf_ptr + read_size);
    // data has been copied, move ptr
    rbuf.buf_ptr += read_size;
    if (bytes_required > bytes_available) {
      // more data needs to be read
      return false;
    }
    // all the data has been read
    rpkt_->InitializePacket();

    return true;
  }

  virtual bool IsEndPacket();

  bool IsEndPacketSuppliment(){
    switch (rpkt_->msg_type) {
      case NetworkMessageType::CLOSE_COMMAND:
      case NetworkMessageType::TERMINATE_COMMAND:
        return true;
      default:
        return false;
    }
  }
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
  };
};

}
}
