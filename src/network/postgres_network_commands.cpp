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
                                callback_func,
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
    interpreter.AddCmdlineOption(key, std::move(value));
  }

  // TODO(Tianyu): Implement authentication. For now we always send AuthOK
  out.WriteStartupResponse();
  interpreter.FinishStartup();
  return Transition::PROCEED;
}

Transition SimpleQueryCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                    PostgresPacketWriter &out,
                                    callback_func callback,
                                    size_t tid) {
  std::string query = in_->ReadString();
  LOG_TRACE("Execute query: %s", query.c_str());
  std::unique_ptr<parser::SQLStatementList> sql_stmt_list;
  try {
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    sql_stmt_list = peloton_parser.BuildParseTree(query);

    // When the query is empty(such as ";" or ";;", still valid),
    // the pare tree is empty, parser will return nullptr.
    if (sql_stmt_list.get() != nullptr && !sql_stmt_list->is_valid) {
      throw ParserException("Error Parsing SQL statement");
    }
  }  // If the statement is invalid or not supported yet
  catch (Exception &e) {
    tcop::ProcessInvalidStatement(interpreter.ClientProcessState());
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR, e.what()}});
    out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
    return Transition::PROCEED;
  }

  if (sql_stmt_list.get() == nullptr ||
      sql_stmt_list->GetNumStatements() == 0) {
    out.WriteEmptyQueryResponse();
    out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
    return Transition::PROCEED;
  }

  // TODO(Yuchen): Hack. We only process the first statement in the packet now.
  // We should store the rest of statements that will not be processed right
  // away. For the hack, in most cases, it works. Because for example in psql,
  // one packet contains only one query. But when using the pipeline mode in
  // Libpqxx, it sends multiple query in one packet. In this case, it's
  // incorrect.
  auto sql_stmt = sql_stmt_list->PassOutStatement(0);

  QueryType query_type =
      StatementTypeToQueryType(sql_stmt->GetType(), sql_stmt.get());
  interpreter.ClientProcessState().protocol_type_ = NetworkProtocolType::POSTGRES_PSQL;

}

} // namespace network
} // namespace peloton