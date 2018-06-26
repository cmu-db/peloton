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
                                callback_func,
                                size_t) {
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
                                    callback_func callback,
                                    size_t tid) {
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  std::string query = in_->ReadString();
  LOG_TRACE("Execute query: %s", query.c_str());
  std::unique_ptr<parser::SQLStatementList> sql_stmt_list;
  try {
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    sql_stmt_list = peloton_parser.BuildParseTree(query);

    // When the query is empty(such as ";" or ";;", still valid),
    // the pare tree is empty, parser will return nullptr.
    if (sql_stmt_list != nullptr && !sql_stmt_list->is_valid) {
      throw ParserException("Error Parsing SQL statement");
    }
  }  // If the statement is invalid or not supported yet
  catch (Exception &e) {
    tcop::ProcessInvalidStatement(state);
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             e.what()}});
    out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
    return Transition::PROCEED;
  }

  if (sql_stmt_list == nullptr ||
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
  std::string stmt_name = "unamed";
  std::unique_ptr<parser::SQLStatementList> unnamed_sql_stmt_list(
      new parser::SQLStatementList());
  unnamed_sql_stmt_list->PassInStatement(std::move(sql_stmt));
  state.statement_ = std::move(tcop::PrepareStatement(state, stmt_name,
      query, std::move(unnamed_sql_stmt_list), tid));
  if (state.statement_ == nullptr) {
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             state.error_message_}});
    out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
    return Transition::PROCEED;
  }
  state.param_values_ = std::vector<type::Value>();
  interpreter.result_format_ =
      std::vector<int>(state.statement_->GetTupleDescriptor().size(), 0);
  auto status = tcop::ExecuteStatement(state,
      state.statement_,
      state.param_values_, false, nullptr,
      interpreter.result_format_, state.result_, tid);
  if (state.is_queuing_) return Transition::PROCEED;
  interpreter.ExecQueryMessageGetResult(status);
  return Transition::PROCEED;
}

Transition ParseCommand::Exec(PostgresProtocolInterpreter &interpreter,
                              PostgresPacketWriter &out,
                              callback_func callback,
                              size_t tid) {
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  std::string statement_name = in_->ReadString(), query = in_->ReadString();

  // In JDBC, one query starts with parsing stage.
  // Reset skipped_stmt_ to false for the new query.

  interpreter.skipped_stmt_ = false;
  std::unique_ptr<parser::SQLStatementList> sql_stmt_list;
  QueryType query_type = QueryType::QUERY_OTHER;
  try {
    LOG_TRACE("%s, %s", statement_name.c_str(), query.c_str());
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    sql_stmt_list = peloton_parser.BuildParseTree(query);
    if (sql_stmt_list.get() != nullptr && !sql_stmt_list->is_valid) {
      throw ParserException("Error parsing SQL statement");
    }
  } catch (Exception &e) {
    tcop::ProcessInvalidStatement(state);
    interpreter.skipped_stmt_ = true;
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR, e.what()}});
    return Transition::PROCEED;
  }

  // If the query is not supported yet,
  // we will skip the rest commands (B,E,..) for this query
  // For empty query, we still want to get it constructed
  // TODO (Tianyi) Consider handle more statement
  bool empty = (sql_stmt_list == nullptr ||
      sql_stmt_list->GetNumStatements() == 0);
  if (!empty) {
    parser::SQLStatement *sql_stmt = sql_stmt_list->GetStatement(0);
    query_type = StatementTypeToQueryType(sql_stmt->GetType(), sql_stmt);
  }
  bool skip = !interpreter.HardcodedExecuteFilter(query_type);
  if (skip) {
    interpreter.skipped_stmt_ = true;
    interpreter.skipped_query_string_ = query;
    interpreter.skipped_query_type_ = query_type;
    out.BeginPacket(NetworkMessageType::PARSE_COMPLETE).EndPacket();
    return Transition::PROCEED;
  }

  // Prepare statement
  std::shared_ptr<Statement> statement(nullptr);

  statement = tcop::PrepareStatement(state, statement_name, query,
                                             std::move(sql_stmt_list));
  if (statement == nullptr) {
    tcop::ProcessInvalidStatement(state);
    interpreter.skipped_stmt_ = true;
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             state.error_message_}});
    return Transition::PROCEED;
  }
  LOG_TRACE("PrepareStatement[%s] => %s", statement_name.c_str(),
            query.c_str());
  auto num_params = in_->ReadValue<uint16_t>();
  // Read param types
  std::vector<PostgresValueType > param_types = ReadParamTypes();

  // Cache the received query
  bool unnamed_query = statement_name.empty();
  statement->SetParamTypes(param_types);

  // Stat
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(
      settings::SettingId::stats_mode)) != StatsType::INVALID) {
    // Make a copy of param types for stat collection
    stats::QueryMetric::QueryParamBuf query_type_buf;
    query_type_buf.len = type_buf_len;
    query_type_buf.buf = PacketCopyBytes(type_buf_begin, type_buf_len);

    // Unnamed statement
    if (unnamed_query) {
      unnamed_stmt_param_types_ = query_type_buf;
    } else {
      statement_param_types_[statement_name] = query_type_buf;
    }
  }

  // Cache the statement
  statement_cache_.AddStatement(statement);

  // Send Parse complete response
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = NetworkMessageType::PARSE_COMPLETE;
  responses_.push_back(std::move(response));
}

} // namespace network
} // namespace peloton