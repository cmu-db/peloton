//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_network_commands.cpp
//
// Identification: src/network/postgres_network_commands.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "parser/postgresparser.h"
#include "network/postgres_protocol_interpreter.h"
#include "network/peloton_server.h"
#include "network/postgres_network_commands.h"
#include "traffic_cop/tcop.h"

#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) ((x) >> 16)

namespace peloton {
namespace network {

// TODO(Tianyu): This is a refactor in progress.
// A lot of the code here should really be moved to traffic cop, and a lot of
// the code here can honestly just be deleted. This is going to be a larger
// project though, so I want to do the architectural refactor first.
std::vector<PostgresValueType> PostgresNetworkCommand::ReadParamTypes()  {
  std::vector<PostgresValueType> result;
  auto num_params = in_->ReadValue<uint16_t>();
  for (uint16_t i = 0; i < num_params; i++)
    result.push_back(in_->ReadValue<PostgresValueType>());
  return result;
}

std::vector<int16_t> PostgresNetworkCommand::ReadParamFormats()  {
  std::vector<int16_t> result;
  auto num_formats = in_->ReadValue<uint16_t>();
  for (uint16_t i = 0; i < num_formats; i++)
    result.push_back(in_->ReadValue<int16_t>());
  return result;
}

void PostgresNetworkCommand::ReadParamValues(std::vector<BindParameter> &bind_parameters,
                                             std::vector<type::Value> &param_values,
                                             const std::vector<PostgresValueType> &param_types,
                                             const std::vector<int16_t> &formats)  {
  auto num_params = in_->ReadValue<uint16_t>();
  for (uint16_t i = 0; i < num_params; i++) {
    auto param_len = in_->ReadValue<int32_t>();
    if (param_len == -1) {
      // NULL
      auto peloton_type = PostgresValueTypeToPelotonValueType(param_types[i]);
      bind_parameters.emplace_back(peloton_type,
                                   std::string(""));
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

void PostgresNetworkCommand::ProcessTextParamValue(std::vector<BindParameter> &bind_parameters,
                                                   std::vector<type::Value> &param_values,
                                                   PostgresValueType type,
                                                   int32_t len)  {
  std::string val = in_->ReadString((size_t) len);
  bind_parameters.emplace_back(type::TypeId::VARCHAR, val);
  param_values.push_back(
      PostgresValueTypeToPelotonValueType(type) == type::TypeId::VARCHAR
      ? type::ValueFactory::GetVarcharValue(val)
      : type::ValueFactory::GetVarcharValue(val).CastAs(
          PostgresValueTypeToPelotonValueType(type)));
}

void PostgresNetworkCommand::ProcessBinaryParamValue(std::vector<BindParameter> &bind_parameters,
                                                     std::vector<type::Value> &param_values,
                                                     PostgresValueType type,
                                                     int32_t len)  {
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
      bind_parameters.emplace_back(type::TypeId::SMALLINT, std::to_string(int_val));
      param_values.push_back(
          type::ValueFactory::GetSmallIntValue(int_val).Copy());
      break;
    }
    case PostgresValueType::INTEGER: {
      PELOTON_ASSERT(len == sizeof(int32_t));
      auto val = in_->ReadValue<int32_t>();
      bind_parameters.emplace_back(type::TypeId::INTEGER, std::to_string(val));
      param_values.push_back(
          type::ValueFactory::GetIntegerValue(val).Copy());
      break;
    }
    case PostgresValueType::BIGINT: {
      PELOTON_ASSERT(len == sizeof(int64_t));
      auto val = in_->ReadValue<int64_t>();
      bind_parameters.emplace_back(type::TypeId::BIGINT, std::to_string(val));
      param_values.push_back(
          type::ValueFactory::GetBigIntValue(val).Copy());
      break;
    }
    case PostgresValueType::DOUBLE: {
      PELOTON_ASSERT(len == sizeof(double));
      auto val = in_->ReadValue<double>();
      bind_parameters.emplace_back(type::TypeId::DECIMAL, std::to_string(val));
      param_values.push_back(
          type::ValueFactory::GetDecimalValue(val).Copy());
      break;
    }
    case PostgresValueType::VARBINARY: {
      auto val = in_->ReadString((size_t) len);
      bind_parameters.emplace_back(type::TypeId::VARBINARY, val);
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


Transition StartupCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                PostgresPacketWriter &out,
                                CallbackFunc) {
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  auto proto_version = in_->ReadValue<uint32_t>();
  LOG_INFO("protocol version: %d", proto_version);
  // SSL initialization
  if (proto_version == SSL_MESSAGE_VERNO) {
    // TODO(Tianyu): Should this be moved from PelotonServer into settings?
    if (PelotonServer::GetSSLLevel() == SSLLevel::SSL_DISABLE) {
      out.WriteSingleBytePacket(NetworkMessageType::SSL_NO);
      return Transition::PROCEED;
    }
    out.WriteSingleBytePacket(NetworkMessageType::SSL_YES);
    return Transition::NEED_SSL_HANDSHAKE;
  }

  // Process startup packet
  if (PROTO_MAJOR_VERSION(proto_version) != 3) {
    LOG_ERROR("Protocol error: only protocol version 3 is supported");
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             "Protocol Version Not Supported"}});
    return Transition::TERMINATE;
  }

  while (in_->HasMore()) {
    // TODO(Tianyu): We don't seem to really handle the other flags?
    std::string key = in_->ReadString(), value = in_->ReadString();
    LOG_TRACE("Option key %s, value %s", key.c_str(), value.c_str());
    if (key == std::string("database"))
      state.db_name_ = value;
    interpreter.AddCmdlineOption(key, std::move(value));
  }

  // TODO(Tianyu): Implement authentication. For now we always send AuthOK
  out.WriteStartupResponse();
  interpreter.FinishStartup();
  return Transition::PROCEED;
}

Transition SimpleQueryCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                    PostgresPacketWriter &out,
                                    CallbackFunc callback) {
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  std::string query = in_->ReadString();
  LOG_TRACE("Execute query: %s", query.c_str());
  if(!tcop::PrepareStatement(state, query)) {
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             state.error_message_}});
    out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
    return Transition::PROCEED;
  }
  state.param_values_ = std::vector<type::Value>();
  interpreter.result_format_ =
      std::vector<int>(state.statement_->GetTupleDescriptor().size(), 0);
  auto status = tcop::ExecuteStatement(state,
      interpreter.result_format_, state.result_, callback);

  if (status == ResultType::QUEUING) return Transition::NEED_RESULT;

  interpreter.ExecQueryMessageGetResult(status);

  return Transition::PROCEED;
}

Transition ParseCommand::Exec(PostgresProtocolInterpreter &interpreter,
                              PostgresPacketWriter &out,
                              CallbackFunc) {
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  std::string statement_name = in_->ReadString(), query = in_->ReadString();
  LOG_TRACE("Execute query: %s", query.c_str());

  if(!tcop::PrepareStatement(state, query, statement_name)) {
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             state.error_message_}});
    out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
    return Transition::PROCEED;
  }
  LOG_TRACE("PrepareStatement[%s] => %s", statement_name.c_str(),
            query.c_str());

  // Read param types
  state.statement_->SetParamTypes(ReadParamTypes());

  // Send Parse complete response
  out.BeginPacket(NetworkMessageType::PARSE_COMPLETE).EndPacket();
  return Transition::PROCEED;
}

} // namespace network
} // namespace peloton