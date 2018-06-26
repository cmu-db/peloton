//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_network_commands.h
//
// Identification: src/include/network/postgres_network_commands.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include <utility>
#include "type/value_factory.h"
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "network/network_types.h"
#include "traffic_cop/traffic_cop.h"
#include "network/marshal.h"
#include "network/postgres_protocol_utils.h"

#define DEFINE_COMMAND(name, flush)                                        \
class name : public PostgresNetworkCommand {                               \
 public:                                                                   \
  explicit name(std::shared_ptr<ReadBuffer> in)                            \
     : PostgresNetworkCommand(std::move(in), flush) {}                     \
  virtual Transition Exec(PostgresProtocolInterpreter &,                   \
                          PostgresPacketWriter &,                          \
                          callback_func) override;                         \
}

namespace peloton {
namespace network {

class PostgresProtocolInterpreter;

class PostgresNetworkCommand {
 public:
  virtual Transition Exec(PostgresProtocolInterpreter &interpreter,
                          PostgresPacketWriter &out,
                          callback_func callback) = 0;

  inline bool FlushOnComplete() { return flush_on_complete_; }

 protected:
  explicit PostgresNetworkCommand(std::shared_ptr<ReadBuffer> in, bool flush)
      : in_(std::move(in)), flush_on_complete_(flush) {}

  inline std::vector<PostgresValueType> ReadParamTypes() {
    std::vector<PostgresValueType> result;
    auto num_params = in_->ReadValue<uint16_t>();
    for (uint16_t i = 0; i < num_params; i++)
      result.push_back(in_->ReadValue<PostgresValueType>());
    return result;
  }

  inline std::vector<int16_t> ReadParamFormats() {
    std::vector<int16_t> result;
    auto num_formats = in_->ReadValue<uint16_t>();
    for (uint16_t i = 0; i < num_formats; i++)
      result.push_back(in_->ReadValue<int16_t>());
    return result;
  }

  // Why are bind parameter and param values different?
  void ReadParamValues(std::vector<std::pair<type::TypeId, std::string>> &bind_parameters,
                       std::vector<type::Value> &param_values,
                       const std::vector<PostgresValueType> &param_types,
                       const std::vector<int16_t> &formats) {
    auto num_params = in_->ReadValue<uint16_t>();
    for (uint16_t i = 0; i < num_params; i++) {
      auto param_len = in_->ReadValue<int32_t>();
      if (param_len == -1) {
        // NULL
        auto peloton_type = PostgresValueTypeToPelotonValueType(param_types[i]);
        bind_parameters.push_back(std::make_pair(peloton_type,
                                                 std::string("")));
        param_values.push_back(type::ValueFactory::GetNullValueByType(
            peloton_type));
      } else {
        (formats[i] == 0)
        ? ProcessTextParamValue(bind_parameters,
                                param_values,
                                param_types[i],
                                param_len)
        : ProcessBinaryParamValue(bind_parameters,
                                  param_values,
                                  param_types[i],
                                  param_len);
      }
    }
  }

  void ProcessTextParamValue(std::vector<std::pair<type::TypeId,
                                                   std::string>> &bind_parameters,
                             std::vector<type::Value> &param_values,
                             PostgresValueType type,
                             int32_t len) {
    std::string val = in_->ReadString((size_t) len);
    bind_parameters.emplace_back(type::TypeId::VARCHAR, val);
    param_values.push_back(
        PostgresValueTypeToPelotonValueType(type) == type::TypeId::VARCHAR
        ? type::ValueFactory::GetVarcharValue(val)
        : type::ValueFactory::GetVarcharValue(val).CastAs(
            PostgresValueTypeToPelotonValueType(type)));
  }

  void ProcessBinaryParamValue(std::vector<std::pair<type::TypeId,
                                                     std::string>> &bind_parameters,
                               std::vector<type::Value> &param_values,
                               PostgresValueType type,
                               int32_t len) {
    switch (type) {
      case PostgresValueType::TINYINT: {
        PELOTON_ASSERT(len == sizeof(int8_t));
        auto val = in_->ReadValue<int8_t>();
        bind_parameters.emplace_back(type::TypeId::TINYINT, std::to_string(val));
        param_values.push_back(
            type::ValueFactory::GetTinyIntValue(val).Copy());
        break;
      }
      case PostgresValueType::SMALLINT: {
        PELOTON_ASSERT(len == sizeof(int16_t));
        auto int_val = in_->ReadValue<int16_t>();
        bind_parameters.push_back(
            std::make_pair(type::TypeId::SMALLINT, std::to_string(int_val)));
        param_values.push_back(
            type::ValueFactory::GetSmallIntValue(int_val).Copy());
        break;
      }
      case PostgresValueType::INTEGER: {
        PELOTON_ASSERT(len == sizeof(int32_t));
        auto val = in_->ReadValue<int32_t>();
        bind_parameters.push_back(
            std::make_pair(type::TypeId::INTEGER, std::to_string(val)));
        param_values.push_back(
            type::ValueFactory::GetIntegerValue(val).Copy());
        break;
      }
      case PostgresValueType::BIGINT: {
        PELOTON_ASSERT(len == sizeof(int64_t));
        auto val = in_->ReadValue<int64_t>();
        bind_parameters.push_back(
            std::make_pair(type::TypeId::BIGINT, std::to_string(val)));
        param_values.push_back(
            type::ValueFactory::GetBigIntValue(val).Copy());
        break;
      }
      case PostgresValueType::DOUBLE: {
        PELOTON_ASSERT(len == sizeof(double));
        auto val = in_->ReadValue<double>();
        bind_parameters.push_back(
            std::make_pair(type::TypeId::DECIMAL, std::to_string(val)));
        param_values.push_back(
            type::ValueFactory::GetDecimalValue(val).Copy());
        break;
      }
      case PostgresValueType::VARBINARY: {
        auto val = in_->ReadString((size_t) len);
        bind_parameters.push_back(
            std::make_pair(type::TypeId::VARBINARY, val));
        param_values.push_back(
            type::ValueFactory::GetVarbinaryValue(
                reinterpret_cast<const uchar *>(val.c_str()),
                len,
                true));
        break;
      }
      default:
        throw NetworkProcessException("Binary Postgres protocol does not support data type "
                                      + PostgresValueTypeToString(type));
    }
  }

  std::shared_ptr<ReadBuffer> in_;
 private:
  bool flush_on_complete_;
};

DEFINE_COMMAND(StartupCommand, true);
DEFINE_COMMAND(SimpleQueryCommand, true);
DEFINE_COMMAND(ParseCommand, false);
DEFINE_COMMAND(BindCommand, false);
DEFINE_COMMAND(DescribeCommand, false);
DEFINE_COMMAND(ExecuteCommand, false);
DEFINE_COMMAND(SyncCommand, true);
DEFINE_COMMAND(CloseCommand, false);
DEFINE_COMMAND(TerminateCommand, true);
DEFINE_COMMAND(NullCommand, true);

} // namespace network
} // namespace peloton
