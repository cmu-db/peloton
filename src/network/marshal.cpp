//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// marshal.cpp
//
// Identification: src/network/marshal.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "network/marshal.h"
#include <algorithm>
#include <cstring>
#include <iterator>

#include <netinet/in.h>

namespace peloton {
namespace network {

// checks for parsing overflows
inline void CheckOverflow(UNUSED_ATTRIBUTE InputPacket *rpkt,
                          UNUSED_ATTRIBUTE size_t size) {
  LOG_TRACE("request->len: %lu", rpkt->len);
  PELOTON_ASSERT(rpkt->ptr + size - 1 < rpkt->len);
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
  PELOTON_MEMCPY(result, &(*begin), len);
  return result;
}

void PacketPutByte(OutputPacket *pkt, const uchar c) {
  pkt->buf.push_back(c);
  pkt->len++;
}

void PacketPutStringWithTerminator(OutputPacket *pkt, const std::string &str) {
  pkt->buf.insert(std::end(pkt->buf), std::begin(str), std::end(str));
  // add null character
  pkt->buf.push_back(0);
  // add 1 for null character
  pkt->len += str.size() + 1;
}

void PacketPutString(OutputPacket *pkt, const std::string &data) {
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

size_t OldReadParamType(
    InputPacket *pkt, int num_params, std::vector<int32_t> &param_types) {
  auto begin = pkt->ptr;
  // get the type of each parameter
  for (int i = 0; i < num_params; i++) {
    int param_type = PacketGetInt(pkt, 4);
    param_types[i] = param_type;
  }
  auto end = pkt->ptr;
  return end - begin;
}

size_t OldReadParamFormat(InputPacket *pkt,
                          int num_params_format,
                          std::vector<int16_t> &formats) {
  auto begin = pkt->ptr;
  // get the format of each parameter
  for (int i = 0; i < num_params_format; i++) {
    formats[i] = PacketGetInt(pkt, 2);
  }
  auto end = pkt->ptr;
  return end - begin;
}

// For consistency, this function assumes the input vectors has the correct size
size_t OldReadParamValue(
    InputPacket *pkt, int num_params, std::vector<int32_t> &param_types,
    std::vector<std::pair<type::TypeId, std::string>> &bind_parameters,
    std::vector<type::Value> &param_values, std::vector<int16_t> &formats) {
  auto begin = pkt->ptr;
  ByteBuf param;
  for (int param_idx = 0; param_idx < num_params; param_idx++) {
    int param_len = PacketGetInt(pkt, 4);
    // BIND packet NULL parameter case
    if (param_len == -1) {
      // NULL mode
      auto peloton_type = PostgresValueTypeToPelotonValueType(
          static_cast<PostgresValueType>(param_types[param_idx]));
      bind_parameters[param_idx] =
          std::make_pair(peloton_type, std::string(""));
      param_values[param_idx] =
          type::ValueFactory::GetNullValueByType(peloton_type);
    } else {
      PacketGetBytes(pkt, param_len, param);

      if (formats[param_idx] == 0) {
        // TEXT mode
        std::string param_str = std::string(std::begin(param), std::end(param));
        bind_parameters[param_idx] =
            std::make_pair(type::TypeId::VARCHAR, param_str);
        if ((unsigned int)param_idx >= param_types.size() ||
            PostgresValueTypeToPelotonValueType(
                (PostgresValueType)param_types[param_idx]) ==
                type::TypeId::VARCHAR) {
          param_values[param_idx] =
              type::ValueFactory::GetVarcharValue(param_str);
        } else {
          param_values[param_idx] =
              (type::ValueFactory::GetVarcharValue(param_str))
                  .CastAs(PostgresValueTypeToPelotonValueType(
                      (PostgresValueType)param_types[param_idx]));
        }
        PELOTON_ASSERT(param_values[param_idx].GetTypeId() !=
            type::TypeId::INVALID);
      } else {
        // BINARY mode
        PostgresValueType pg_value_type =
            static_cast<PostgresValueType>(param_types[param_idx]);
        LOG_TRACE("Postgres Protocol Conversion [param_idx=%d]", param_idx);
        switch (pg_value_type) {
          case PostgresValueType::TINYINT: {
            int8_t int_val = 0;
            for (size_t i = 0; i < sizeof(int8_t); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters[param_idx] =
                std::make_pair(type::TypeId::TINYINT, std::to_string(int_val));
            param_values[param_idx] =
                type::ValueFactory::GetTinyIntValue(int_val).Copy();
            break;
          }
          case PostgresValueType::SMALLINT: {
            int16_t int_val = 0;
            for (size_t i = 0; i < sizeof(int16_t); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters[param_idx] =
                std::make_pair(type::TypeId::SMALLINT, std::to_string(int_val));
            param_values[param_idx] =
                type::ValueFactory::GetSmallIntValue(int_val).Copy();
            break;
          }
          case PostgresValueType::INTEGER: {
            int32_t int_val = 0;
            for (size_t i = 0; i < sizeof(int32_t); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters[param_idx] =
                std::make_pair(type::TypeId::INTEGER, std::to_string(int_val));
            param_values[param_idx] =
                type::ValueFactory::GetIntegerValue(int_val).Copy();
            break;
          }
          case PostgresValueType::BIGINT: {
            int64_t int_val = 0;
            for (size_t i = 0; i < sizeof(int64_t); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters[param_idx] =
                std::make_pair(type::TypeId::BIGINT, std::to_string(int_val));
            param_values[param_idx] =
                type::ValueFactory::GetBigIntValue(int_val).Copy();
            break;
          }
          case PostgresValueType::DOUBLE: {
            double float_val = 0;
            unsigned long buf = 0;
            for (size_t i = 0; i < sizeof(double); ++i) {
              buf = (buf << 8) | param[i];
            }
            PELOTON_MEMCPY(&float_val, &buf, sizeof(double));
            bind_parameters[param_idx] = std::make_pair(
                type::TypeId::DECIMAL, std::to_string(float_val));
            param_values[param_idx] =
                type::ValueFactory::GetDecimalValue(float_val).Copy();
            break;
          }
          case PostgresValueType::VARBINARY: {
            bind_parameters[param_idx] = std::make_pair(
                type::TypeId::VARBINARY,
                std::string(reinterpret_cast<char *>(&param[0]), param_len));
            param_values[param_idx] = type::ValueFactory::GetVarbinaryValue(
                &param[0], param_len, true);
            break;
          }
          default: {
            LOG_ERROR(
                "Binary Postgres protocol does not support data type '%s' [%d]",
                PostgresValueTypeToString(pg_value_type).c_str(),
                param_types[param_idx]);
            break;
          }
        }
        PELOTON_ASSERT(param_values[param_idx].GetTypeId() !=
            type::TypeId::INVALID);
      }
    }
  }
  auto end = pkt->ptr;
  return end - begin;
}

}  // namespace network
}  // namespace peloton
