//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// marshal.cpp
//
// Identification: src/wire/marshal.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstring>
#include <iterator>

#include "wire/marshal.h"

#include <netinet/in.h>

namespace peloton {
namespace wire {

// checks for parsing overflows
void CheckOverflow(Packet *pkt, size_t size) {
  if (pkt->ptr + size - 1 >= pkt->len) {
    // overflow case, throw error
    LOG_WARN("Parsing error: pointer overflow for int");
  }
}

PktBuf::iterator GetEndItr(Packet *pkt, int len) {
  if (len == 0) return std::end(pkt->buf);
  return std::begin(pkt->buf) + pkt->ptr + len;
}

int PacketGetInt(Packet *pkt, uchar base) {
  int value = 0;

  CheckOverflow(pkt, base);

  switch (base) {
    case 1:  // 8-bit int
      std::copy(pkt->buf.begin() + pkt->ptr, GetEndItr(pkt, base),
                reinterpret_cast<uchar *>(&value));
      break;

    case 2:  // 16-bit int
      std::copy(pkt->buf.begin() + pkt->ptr, GetEndItr(pkt, base),
                reinterpret_cast<uchar *>(&value));
      value = ntohs(value);
      break;

    case 4:  // 32-bit int
      std::copy(pkt->buf.begin() + pkt->ptr, GetEndItr(pkt, base),
                reinterpret_cast<uchar *>(&value));
      value = ntohl(value);
      break;

    default:
      LOG_ERROR("Parsing error: Invalid int base size");
      exit(EXIT_FAILURE);
  }

  // move the pointer
  pkt->ptr += base;
  return value;
}

void PacketGetBytes(Packet *pkt, size_t len, PktBuf &result) {
  result.clear();
  CheckOverflow(pkt, len);

  // return empty vector
  if (len == 0) return;

  result.insert(std::end(result), std::begin(pkt->buf) + pkt->ptr,
                GetEndItr(pkt, len));

  // move the pointer
  pkt->ptr += len;
}

void PacketGetString(Packet *pkt, size_t len, std::string &result) {
  // exclude null char for std string
  result =
      std::string(std::begin(pkt->buf) + pkt->ptr, GetEndItr(pkt, len - 1));
}

void GetStringToken(Packet *pkt, std::string &result) {
  // save start itr position of string
  auto start = std::begin(pkt->buf) + pkt->ptr;

  auto find_itr = std::find(start, std::end(pkt->buf), 0);

  if (find_itr == std::end(pkt->buf)) {
    // no match? consider the remaining vector
    // as a single string and continue
    pkt->ptr = pkt->len;
    result = std::string(std::begin(pkt->buf) + pkt->ptr, std::end(pkt->buf));
    return;
  }

  // update ptr position
  pkt->ptr = find_itr - std::begin(pkt->buf) + 1;

  // edge case
  if (start == find_itr) {
    result = std::string("");
    return;
  }

  result = std::string(start, find_itr);
}

void PacketPutByte(std::unique_ptr<Packet> &pkt, const uchar c) {
  pkt->buf.push_back(c);
  pkt->len++;
}

void PacketPutString(std::unique_ptr<Packet> &pkt, const std::string &str) {
  pkt->buf.insert(std::end(pkt->buf), std::begin(str), std::end(str));
  // add null character
  pkt->buf.push_back(0);
  // add 1 for null character
  pkt->len += str.size() + 1;
}

void PacketPutBytes(std::unique_ptr<Packet> &pkt,
                    const std::vector<uchar> &data) {
  pkt->buf.insert(std::end(pkt->buf), std::begin(data), std::end(data));
  pkt->len += data.size();
}

void PacketPutInt(std::unique_ptr<Packet> &pkt, int n, int base) {
  switch (base) {
    case 2:
      n = htons(n);
      break;

    case 4:
      n = htonl(n);
      break;

    default:
      LOG_ERROR("Parsing error: Invalid base for int");
      exit(EXIT_FAILURE);
  }

  PacketPutCbytes(pkt, reinterpret_cast<uchar *>(&n), base);
}

void PacketPutCbytes(std::unique_ptr<Packet> &pkt, const uchar *b, int len) {
  pkt->buf.insert(std::end(pkt->buf), b, b + len);
  pkt->len += len;
}

/*
 * read_packet - Tries to read a single packet, returns true on success,
 * 		false on failure. Accepts pointer to an empty packet, and if the
 * 		expected packet contains a type field. The function does a preliminary
 * 		read to fetch the size value and then reads the rest of the packet.
 *
 * 		Assume: Packet length field is always 32-bit int
 */

bool ReadPacket(Packet *pkt, bool has_type_field, Client *client) {
  uint32_t pkt_size = 0, initial_read_size = sizeof(int32_t);

  if (has_type_field)
    // need to read type character as well
    initial_read_size++;

  // reads the type and size of packet
  PktBuf init_pkt;

  // read first size_field_end bytes
  if (!client->sock->ReadBytes(init_pkt,
                               static_cast<size_t>(initial_read_size))) {
    // nothing more to read
    return false;
  }

  if (has_type_field) {
    // packet includes type byte as well
    pkt->msg_type = init_pkt[0];

    // extract packet size
    std::copy(init_pkt.begin() + 1, init_pkt.end(),
              reinterpret_cast<uchar *>(&pkt_size));
  } else {
    // directly extract packet size
    std::copy(init_pkt.begin(), init_pkt.end(),
              reinterpret_cast<uchar *>(&pkt_size));
  }

  // packet size includes initial bytes read as well
  pkt_size = ntohl(pkt_size) - sizeof(int32_t);

  if (!client->sock->ReadBytes(pkt->buf, static_cast<size_t>(pkt_size))) {
    // nothing more to read
    return false;
  }

  pkt->len = pkt_size;

  return true;
}

bool WritePackets(std::vector<std::unique_ptr<Packet>> &packets,
                  Client *client) {
  // iterate through all the packets
  for (size_t i = 0; i < packets.size(); i++) {
    auto pkt = packets[i].get();
    if (!client->sock->BufferWriteBytes(pkt->buf, pkt->len, pkt->msg_type)) {
      packets.clear();
      return false;
    }
  }

  // clear packets
  packets.clear();
  return client->sock->FlushWriteBuffer();
}

}  // end wire
}  // end peloton
