//
// Created by tim on 25/07/17.
//

#include "networking/packet_manager.h"

namespace peloton {
namespace networking {

PacketManager::PacketManagerResultType
PostgresPacketManager::GetPacketFromBuffer(
    Buffer &rbuf, std::unique_ptr <InputPacket> &rpkt) {

  if (rpkt_ == nullptr) {
    rpkt_ = std::make_unique<InputPacket>();
  }

  if (rpkt_->header_parsed == false) {
    if (!ReadPacketHeader(rbuf)) {
      //Header cannot be loaded;
      return PacketNotDone;
    }
  }

  if (!ReadPacket(rbuf)) {
    return PacketNotDone;
  }

  rpkt = std::move(rpkt_);

  if (IsEndPacket()) {
    return PacketEnd;
  }

  return PacketDone;
}

void PostgresPacketManager::GetSizeFromPktHeader(
    size_t start_index, Buffer &rbuf) {
  rpkt_->len = 0;
  // directly converts from network byte order to little-endian
  for (size_t i = start_index; i < start_index + sizeof(uint32_t); i++) {
    rpkt_->len = (rpkt_->.len << 8) | rbuf.GetByte(i);
  }
  // packet size includes initial bytes read as well
}

bool PostgresPacketManager::ReadPacketHeader(
    Buffer &rbuf) {
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
bool PostgresPacketManager::ReadPacket(Buffer &rbuf) {
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

bool PostgresPacketManager::IsEndPacketSuppliment() {
  switch (rpkt_->msg_type) {
    case NetworkMessageType::CLOSE_COMMAND:
    case NetworkMessageType::TERMINATE_COMMAND:return true;
    default:return false;
  }
}

}
}
