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
Transition StartupCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                PostgresPacketWriter &out,
                                callback_func) {
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
                                    callback_func callback) {
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
                              callback_func) {
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