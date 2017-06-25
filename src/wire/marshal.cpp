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
#include "common/macros.h"

#include <netinet/in.h>

namespace peloton {
namespace wire {

// checks for parsing overflows
inline void CheckOverflow(UNUSED_ATTRIBUTE InputPacket *rpkt,
                          UNUSED_ATTRIBUTE size_t size) {
  LOG_TRACE("rpkt->len: %lu", rpkt->len);
  PL_ASSERT(rpkt->ptr + size - 1 < rpkt->len);
}

int PacketGetInt(InputPacket *rpkt, uchar base) {
  int value = 0;
  auto begin = rpkt->Begin() + rpkt->ptr;
  auto end = rpkt->Begin() + rpkt->ptr + base;
  CheckOverflow(rpkt, base);

  switch (base) {
    case 1:  // 8-bit int
      std::copy(begin, end, reinterpret_cast<uchar *>(&value));
      break;

    case 2:  // 16-bit int
      std::copy(begin, end, reinterpret_cast<uchar *>(&value));
      value = ntohs(value);
      break;

    case 4:  // 32-bit int
      std::copy(begin, end, reinterpret_cast<uchar *>(&value));
      value = ntohl(value);
      break;

    default:
      LOG_ERROR("Parsing error: Invalid int base size");
      exit(EXIT_FAILURE);
  }

  // move the pointer
  rpkt->ptr += base;
  return value;
}

void PacketGetBytes(InputPacket *rpkt, size_t len, ByteBuf &result) {
  result.clear();
  CheckOverflow(rpkt, len);

  // return empty vector
  if (len == 0) return;

  result.insert(std::end(result), rpkt->Begin() + rpkt->ptr,
                rpkt->Begin() + rpkt->ptr + len);

  // move the pointer
  rpkt->ptr += len;
}

void PacketGetByte(InputPacket *rpkt, uchar &result) {
  // access the current byte
  result = *(rpkt->Begin() + rpkt->ptr);
  // move pointer
  rpkt->ptr++;
}

void PacketGetString(InputPacket *rpkt, size_t len, std::string &result) {
  // return empty string
  if (len == 0) return;

  // exclude null char for std string
  result = std::string(rpkt->Begin() + rpkt->ptr,
                       rpkt->Begin() + rpkt->ptr + len - 1);
  rpkt->ptr += len;
}

void GetStringToken(InputPacket *rpkt, std::string &result) {
  // save start itr position of string
  auto start = rpkt->Begin() + rpkt->ptr;

  auto find_itr = std::find(start, rpkt->End(), 0);

  if (find_itr == rpkt->End()) {
    // no match? consider the remaining vector
    // as a single string and continue
    result = std::string(rpkt->Begin() + rpkt->ptr, rpkt->End());
    rpkt->ptr = rpkt->len;
    return;
  } else {
    // update ptr position
    rpkt->ptr = find_itr - rpkt->Begin() + 1;

    // edge case
    if (start == find_itr) {
      result = std::string("");
      return;
    }

    result = std::string(start, find_itr);
  }
}

uchar *PacketCopyBytes(ByteBuf::const_iterator begin, int len) {
  uchar *result = new uchar[len];
  PL_MEMCPY(result, &(*begin), len);
  return result;
}

void PacketPutByte(OutputPacket *pkt, const uchar c) {
  pkt->buf.push_back(c);
  pkt->len++;
}

void PacketPutString(OutputPacket *pkt, const std::string &str) {
  pkt->buf.insert(std::end(pkt->buf), std::begin(str), std::end(str));
  // add null character
  pkt->buf.push_back(0);
  // add 1 for null character
  pkt->len += str.size() + 1;
}

void PacketPutBytes(OutputPacket *pkt, const std::vector<uchar> &data) {
  pkt->buf.insert(std::end(pkt->buf), std::begin(data), std::end(data));
  pkt->len += data.size();
}

void PacketPutInt(OutputPacket *pkt, int n, int base) {
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

void PacketPutCbytes(OutputPacket *pkt, const uchar *b, int len) {
  pkt->buf.insert(std::end(pkt->buf), b, b + len);
  pkt->len += len;
}

}  // end wire
}  // end peloton
