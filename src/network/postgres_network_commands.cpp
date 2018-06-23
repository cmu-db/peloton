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

Transition StartupCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                PostgresPacketWriter &out,
                                size_t) {
  auto proto_version = in_->ReadInt<uint32_t>();
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
      interpreter.ClientProcessState().db_name_ = value;
    interpreter.AddCmdlineOption(std::move(key), std::move(value));
  }

  // TODO(Tianyu): Implement authentication. For now we always send AuthOK
  out.WriteStartupResponse();
  interpreter.FinishStartup();
  return Transition::PROCEED;
}

Transition ParseCommand::Exec(PostgresProtocolInterpreter &interpreter,
                              PostgresPacketWriter &out,
                              size_t) {
  // TODO(Tianyu): Figure out what skipped stmt does and maybe implement
  std::string statement_name = in_->ReadString(), query = in_->ReadString();
  std::unique_ptr<parser::SQLStatementList> sql_stmt_list;
  try {
    sql_stmt_list = tcop::ParseQuery(interpreter.ClientProcessState(), query);
  } catch (Exception &e) {
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR, e.what()}});
    return Transition::PROCEED;
  }

  auto statement =
}

} // namespace network
} // namespace peloton