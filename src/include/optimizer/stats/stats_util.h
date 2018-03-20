//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_util.h
//
// Identification: src/include/optimizer/stats/stats_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <murmur3/MurmurHash3.h>

#include "common/macros.h"
#include "type/type.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace optimizer {

class StatsUtil {
 public:
  // Convert numeric peloton value type to primitive value.
  // Return NaN if value is not numeric.
  static double PelotonValueToNumericValue(const type::Value& value) {
    double raw_value = std::numeric_limits<double>::quiet_NaN();
    if (value.IsNull()) {
      LOG_TRACE("Fail to convert Peloton NULL value to numeric value.");
      return raw_value;
    }
    if (value.CheckInteger() || value.GetTypeId() == type::TypeId::TIMESTAMP) {
      raw_value = static_cast<double>(value.GetAs<int>());
    } else if (value.GetTypeId() == type::TypeId::DECIMAL) {
      raw_value = value.GetAs<double>();
    } else {
      LOG_TRACE("Fail to convert non-numeric Peloton value to numeric value");
    }
    return raw_value;
  }

  /*
   * Default type::Value hash uses std::hash, and here we want to
   * use cusotmized hash functions. Currently we are using
   * Murmur3 and in the future we want to try Farmhash.
   */
  static uint64_t HashValue(const type::Value& value) {
    uint64_t hash[2];
    switch (value.GetTypeId()) {
      case type::TypeId::VARCHAR:
      case type::TypeId::VARBINARY: {
        const char* key = value.GetData();
        MurmurHash3_x64_128(key, (uint64_t)strlen(key), 0, hash);
      } break;
      case type::TypeId::BOOLEAN:
      case type::TypeId::TINYINT: {
        int8_t key = value.GetAs<int8_t>();
        MurmurHash3_x64_128(&key, sizeof(key), 0, hash);
      } break;
      case type::TypeId::SMALLINT: {
        int16_t key = value.GetAs<int16_t>();
        MurmurHash3_x64_128(&key, sizeof(key), 0, hash);
      } break;
      case type::TypeId::INTEGER: {
        int32_t key = value.GetAs<int32_t>();
        MurmurHash3_x64_128(&key, sizeof(key), 0, hash);
      } break;
      case type::TypeId::BIGINT: {
        int64_t key = value.GetAs<int64_t>();
        MurmurHash3_x64_128(&key, sizeof(key), 0, hash);
      } break;
      case type::TypeId::DECIMAL: {
        double key = value.GetAs<double>();
        MurmurHash3_x64_128(&key, sizeof(key), 0, hash);
      } break;
      case type::TypeId::DATE: {
        uint32_t key = value.GetAs<uint32_t>();
        MurmurHash3_x64_128(&key, sizeof(key), 0, hash);
      } break;
      case type::TypeId::TIMESTAMP: {
        uint64_t key = value.GetAs<uint64_t>();
        MurmurHash3_x64_128(&key, sizeof(key), 0, hash);
      } break;
      default:
        // Hack for other data types.
        std::string value_str = value.ToString();
        const char* key = value_str.c_str();
        MurmurHash3_x64_128(key, (uint64_t)strlen(key), 0, hash);
    }
    return hash[0];
  }
};
}  // namespace optimizer
}  // namespace peloton
