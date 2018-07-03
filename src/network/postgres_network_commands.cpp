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
#include "settings/settings_manager.h"
#include "planner/abstract_plan.h"

namespace peloton {
namespace network {

// TODO(Tianyu): This is a refactor in progress.
// A lot of the code here should really be moved to traffic cop, and a lot of
// the code here can honestly just be deleted. This is going to be a larger
// project though, so I want to do the architectural refactor first.
std::vector<PostgresValueType> PostgresNetworkCommand::ReadParamTypes() {
  std::vector<PostgresValueType> result;
  auto num_params = in_.ReadValue<uint16_t>();
  for (uint16_t i = 0; i < num_params; i++)
    result.push_back(in_.ReadValue<PostgresValueType>());
  return result;
}

std::vector<PostgresDataFormat> PostgresNetworkCommand::ReadParamFormats() {
  std::vector<PostgresDataFormat> result;
  auto num_formats = in_.ReadValue<uint16_t>();
  for (uint16_t i = 0; i < num_formats; i++)
    result.push_back(in_.ReadValue<PostgresDataFormat>());
  return result;
}

void PostgresNetworkCommand::ReadParamValues(std::vector<BindParameter> &bind_parameters,
                                             std::vector<type::Value> &param_values,
                                             const std::vector<PostgresValueType> &param_types,
                                             const std::vector<
                                                 PostgresDataFormat> &formats) {
  auto num_params = in_.ReadValue<uint16_t>();
  for (uint16_t i = 0; i < num_params; i++) {
    auto param_len = in_.ReadValue<int32_t>();
    if (param_len == -1) {
      // NULL
      auto peloton_type = PostgresValueTypeToPelotonValueType(param_types[i]);
      bind_parameters.emplace_back(peloton_type,
                                   std::string(""));
      param_values.push_back(type::ValueFactory::GetNullValueByType(
          peloton_type));
    } else
      switch (formats[i]) {
        case PostgresDataFormat::TEXT:
          ProcessTextParamValue(bind_parameters,
                                param_values,
                                param_types[i],
                                param_len);
          break;
        case PostgresDataFormat::BINARY:
          ProcessBinaryParamValue(bind_parameters,
                                  param_values,
                                  param_types[i],
                                  param_len);
          break;
        default:
          throw NetworkProcessException("Unexpected format code");
      }
  }
}

void PostgresNetworkCommand::ProcessTextParamValue(std::vector<BindParameter> &bind_parameters,
                                                   std::vector<type::Value> &param_values,
                                                   PostgresValueType type,
                                                   int32_t len) {
  std::string val = in_.ReadString((size_t) len);
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
                                                     int32_t len) {
  switch (type) {
    case PostgresValueType::TINYINT: {
      PELOTON_ASSERT(len == sizeof(int8_t));
      auto val = in_.ReadValue<int8_t>();
      bind_parameters.emplace_back(type::TypeId::TINYINT, std::to_string(val));
      param_values.push_back(
          type::ValueFactory::GetTinyIntValue(val).Copy());
      break;
    }
    case PostgresValueType::SMALLINT: {
      PELOTON_ASSERT(len == sizeof(int16_t));
      auto int_val = in_.ReadValue<int16_t>();
      bind_parameters.emplace_back(type::TypeId::SMALLINT,
                                   std::to_string(int_val));
      param_values.push_back(
          type::ValueFactory::GetSmallIntValue(int_val).Copy());
      break;
    }
    case PostgresValueType::INTEGER: {
      PELOTON_ASSERT(len == sizeof(int32_t));
      auto val = in_.ReadValue<int32_t>();
      bind_parameters.emplace_back(type::TypeId::INTEGER, std::to_string(val));
      param_values.push_back(
          type::ValueFactory::GetIntegerValue(val).Copy());
      break;
    }
    case PostgresValueType::BIGINT: {
      PELOTON_ASSERT(len == sizeof(int64_t));
      auto val = in_.ReadValue<int64_t>();
      bind_parameters.emplace_back(type::TypeId::BIGINT, std::to_string(val));
      param_values.push_back(
          type::ValueFactory::GetBigIntValue(val).Copy());
      break;
    }
    case PostgresValueType::DOUBLE: {
      PELOTON_ASSERT(len == sizeof(double));
      auto val = in_.ReadValue<double>();
      bind_parameters.emplace_back(type::TypeId::DECIMAL, std::to_string(val));
      param_values.push_back(
          type::ValueFactory::GetDecimalValue(val).Copy());
      break;
    }
    case PostgresValueType::VARBINARY: {
      auto val = in_.ReadString((size_t) len);
      bind_parameters.emplace_back(type::TypeId::VARBINARY, val);
      param_values.push_back(
          type::ValueFactory::GetVarbinaryValue(
              reinterpret_cast<const uchar *>(val.c_str()),
              len,
              true));
      break;
    }
    default:
      throw NetworkProcessException(
          "Binary Postgres protocol does not support data type "
              + PostgresValueTypeToString(type));
  }
}

std::vector<PostgresDataFormat> PostgresNetworkCommand::ReadResultFormats(size_t tuple_size) {
  auto num_format_codes = in_.ReadValue<int16_t>();
  switch (num_format_codes) {
    case 0:
      // Default text mode
      return std::vector<PostgresDataFormat>(tuple_size,
                                             PostgresDataFormat::TEXT);
    case 1:
      return std::vector<PostgresDataFormat>(tuple_size,
                                             in_.ReadValue<PostgresDataFormat>());
    default:std::vector<PostgresDataFormat> result;
      for (auto i = 0; i < num_format_codes; i++)
        result.push_back(in_.ReadValue<PostgresDataFormat>());
      return result;
  }
}

Transition SimpleQueryCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                    PostgresPacketWriter &out,
                                    CallbackFunc callback) {
  interpreter.protocol_type_ = NetworkProtocolType::POSTGRES_PSQL;
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  std::string query = in_.ReadString();
  LOG_TRACE("Execute query: %s", query.c_str());
  std::unique_ptr<parser::SQLStatementList> sql_stmt_list;
  try {
    sql_stmt_list = tcop::Tcop::GetInstance().ParseQuery(query);
  } catch (Exception &e) {
    tcop::Tcop::GetInstance().ProcessInvalidStatement(state);
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             e.what()}});
    out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
    return Transition::PROCEED;
  }

  if (sql_stmt_list == nullptr || sql_stmt_list->GetNumStatements() == 0) {
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

  switch (query_type) {
    case QueryType::QUERY_PREPARE: {
      std::shared_ptr<Statement> statement(nullptr);
      auto prep_stmt = dynamic_cast<parser::PrepareStatement *>(sql_stmt.get());
      std::string stmt_name = prep_stmt->name;
      statement = tcop::Tcop::GetInstance().PrepareStatement(state,
                                                             stmt_name,
                                                             query,
                                                             std::move(prep_stmt->query));
      if (statement == nullptr) {
        out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                                 state.error_message_}});
        out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
        return Transition::PROCEED;
      }
      state.statement_cache_.AddStatement(statement);
      interpreter.CompleteCommand(out, query_type, 0);
      // PAVLO: 2017-01-15
      // There used to be code here that would invoke this method passing
      // in NetworkMessageType::READY_FOR_QUERY as the argument. But when
      // I switched to strong types, this obviously doesn't work. So I
      // switched it to be NetworkTransactionStateType::IDLE. I don't know
      // we just don't always send back the internal txn state?
      out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
      return Transition::PROCEED;
    };
    case QueryType::QUERY_EXECUTE: {
      std::vector<type::Value> param_values;
      auto
          *exec_stmt = dynamic_cast<parser::ExecuteStatement *>(sql_stmt.get());
      std::string stmt_name = exec_stmt->name;

      auto cached_statement = state.statement_cache_.GetStatement(stmt_name);
      if (cached_statement != nullptr)
        state.statement_ = cached_statement;
        // Did not find statement with same name
      else {
        std::string error_message = "The prepared statement does not exist";
        out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                                 "The prepared statement does not exist"}});
        out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
        return Transition::PROCEED;
      }
      state.result_format_ =
          std::vector<PostgresDataFormat>(state.statement_->GetTupleDescriptor().size(),
                                          PostgresDataFormat::TEXT);

      if (!tcop::Tcop::GetInstance().BindParamsForCachePlan(state,
                                                            exec_stmt->parameters)) {
        tcop::Tcop::GetInstance().ProcessInvalidStatement(state);
        out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                                 state.error_message_}});
        out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
        return Transition::PROCEED;
      }

      auto status = tcop::Tcop::GetInstance().ExecuteStatement(state, callback);
      if (state.is_queuing_) return Transition::NEED_RESULT;
      interpreter.ExecQueryMessageGetResult(out, status);
      return Transition::PROCEED;
    };
    case QueryType::QUERY_EXPLAIN: {
      auto status = interpreter.ExecQueryExplain(query,
                                                 dynamic_cast<parser::ExplainStatement &>(*sql_stmt));
      interpreter.ExecQueryMessageGetResult(out, status);
      return Transition::PROCEED;
    }
    default: {
      std::string stmt_name = "unnamed";
      std::unique_ptr<parser::SQLStatementList> unnamed_sql_stmt_list(
          new parser::SQLStatementList());
      unnamed_sql_stmt_list->PassInStatement(std::move(sql_stmt));
      state.statement_ = tcop::Tcop::GetInstance().PrepareStatement(state,
                                                                    stmt_name,
                                                                    query,
                                                                    std::move(
                                                                        unnamed_sql_stmt_list));
      if (state.statement_ == nullptr) {
        out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                                 state.error_message_}});
        out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
        return Transition::PROCEED;
      }
      state.param_values_ = std::vector<type::Value>();
      state.result_format_ =
          std::vector<PostgresDataFormat>(state.statement_->GetTupleDescriptor().size(),
                                          PostgresDataFormat::TEXT);
      auto status =
          tcop::Tcop::GetInstance().ExecuteStatement(state, callback);
      if (state.is_queuing_)
        return Transition::NEED_RESULT;
      interpreter.ExecQueryMessageGetResult(out, status);
      return Transition::PROCEED;
    }
  }
}

Transition ParseCommand::Exec(PostgresProtocolInterpreter &interpreter,
                              PostgresPacketWriter &out,
                              CallbackFunc) {
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  std::string statement_name = in_.ReadString(), query = in_.ReadString();
  // In JDBC, one query starts with parsing stage.
  // Reset skipped_stmt_ to false for the new query.
  state.skipped_stmt_ = false;
  std::unique_ptr<parser::SQLStatementList> sql_stmt_list;
  QueryType query_type = QueryType::QUERY_OTHER;
  try {
    sql_stmt_list = tcop::Tcop::GetInstance().ParseQuery(query);
  } catch (Exception &e) {
    tcop::Tcop::GetInstance().ProcessInvalidStatement(state);
    state.skipped_stmt_ = true;
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             e.what()}});
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
    state.skipped_stmt_ = true;
    state.skipped_query_string_ = query;
    state.skipped_query_type_ = query_type;
    out.WriteSingleTypePacket(NetworkMessageType::PARSE_COMPLETE);
    return Transition::PROCEED;
  }

  auto statement = tcop::Tcop::GetInstance().PrepareStatement(state,
                                                              statement_name,
                                                              query,
                                                              std::move(
                                                                  sql_stmt_list));
  if (statement == nullptr) {
    tcop::Tcop::GetInstance().ProcessInvalidStatement(state);
    state.skipped_stmt_ = true;
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             state.error_message_}});
    return Transition::PROCEED;
  }

  LOG_TRACE("PrepareStatement[%s] => %s", statement_name.c_str(),
            query.c_str());

  // Cache the received query
  statement->SetParamTypes(ReadParamTypes());

  // Cache the statement
  state.statement_cache_.AddStatement(statement);
  out.WriteSingleTypePacket(NetworkMessageType::PARSE_COMPLETE);
  return Transition::PROCEED;
}

Transition BindCommand::Exec(PostgresProtocolInterpreter &interpreter,
                             PostgresPacketWriter &out,
                             CallbackFunc) {
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  std::string portal_name = in_.ReadString(),
      statement_name = in_.ReadString();
  if (state.skipped_stmt_) {
    out.WriteSingleTypePacket(NetworkMessageType::BIND_COMPLETE);
    return Transition::PROCEED;
  }
  std::vector<PostgresDataFormat> formats = ReadParamFormats();


  // Get statement info generated in PARSE message
  std::shared_ptr<Statement>
      statement = state.statement_cache_.GetStatement(statement_name);
  if (statement == nullptr) {
    std::string error_message = statement_name.empty()
                                ? "Invalid unnamed statement"
                                : "The prepared statement does not exist";
    LOG_ERROR("%s", error_message.c_str());
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             error_message}});
    return Transition::PROCEED;
  }

  // Empty query
  if (statement->GetQueryType() == QueryType::QUERY_INVALID) {
    out.BeginPacket(NetworkMessageType::BIND_COMMAND).EndPacket();
    // TODO(Tianyi) This is a hack to respond correct describe message
    // as well as execute message
    state.skipped_stmt_ = true;
    state.skipped_query_string_ = "";
    return Transition::PROCEED;
  }

  const auto &query_string = statement->GetQueryString();
  const auto &query_type = statement->GetQueryType();

  // check if the loaded statement needs to be skipped
  state.skipped_stmt_ = false;
  if (!interpreter.HardcodedExecuteFilter(query_type)) {
    state.skipped_stmt_ = true;
    state.skipped_query_string_ = query_string;
    out.WriteSingleTypePacket(NetworkMessageType::BIND_COMPLETE);
    return Transition::PROCEED;
  }

  // Group the parameter types and the parameters in this vector
  std::vector<std::pair<type::TypeId, std::string>> bind_parameters;
  std::vector<type::Value> param_values;

  auto param_types = statement->GetParamTypes();
  ReadParamValues(bind_parameters, param_values, param_types, formats);
  state.result_format_ =
      ReadResultFormats(statement->GetTupleDescriptor().size());

  if (!param_values.empty())
    statement->GetPlanTree()->SetParameterValues(&param_values);
  // Instead of tree traversal, we should put param values in the
  // executor context.

  interpreter.portals_[portal_name] =
      std::make_shared<Portal>(portal_name, statement, std::move(param_values));
  out.WriteSingleTypePacket(NetworkMessageType::BIND_COMPLETE);
  return Transition::PROCEED;
}

Transition DescribeCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                 PostgresPacketWriter &out,
                                 CallbackFunc) {
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  if (state.skipped_stmt_) {
    // send 'no-data'
    out.WriteSingleTypePacket(NetworkMessageType::NO_DATA_RESPONSE);
    return Transition::PROCEED;
  }

  auto mode = in_.ReadValue<PostgresNetworkObjectType>();
  std::string portal_name = in_.ReadString();
  switch (mode) {
    case PostgresNetworkObjectType::PORTAL: {
      LOG_TRACE("Describe a portal");
      auto portal_itr = interpreter.portals_.find(portal_name);
      // TODO: error handling here
      // Ahmed: This is causing the continuously running thread
      // Changed the function signature to return boolean
      // when false is returned, the connection is closed
      if (portal_itr == interpreter.portals_.end()) {
        LOG_ERROR("Did not find portal : %s", portal_name.c_str());
        // TODO(Tianyu): Why is this thing swallowing error?
        out.WriteTupleDescriptor(std::vector<FieldInfo>());
      } else
        out.WriteTupleDescriptor(portal_itr->second->GetStatement()->GetTupleDescriptor());
      break;
    }
    case PostgresNetworkObjectType::STATEMENT:
      // TODO(Tianyu): Do we not support this or something?
      LOG_TRACE("Describe a prepared statement");
      break;
    default:
      throw NetworkProcessException("Unexpected Describe type");
  }
  return Transition::PROCEED;
}

Transition ExecuteCommand::Exec(PostgresProtocolInterpreter &interpreter,
                                PostgresPacketWriter &out,
                                CallbackFunc callback) {
  interpreter.protocol_type_ = NetworkProtocolType::POSTGRES_JDBC;
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  std::string portal_name = in_.ReadString();
  // We never seem to use this row limit field in the message?
  auto row_limit = in_.ReadValue<int32_t>();
  (void) row_limit;

  // covers weird JDBC edge case of sending double BEGIN statements. Don't
  // execute them
  if (state.skipped_stmt_) {
    if (state.skipped_query_string_ == "")
      out.WriteEmptyQueryResponse();
    else
      interpreter.CompleteCommand(out,
                                  state.skipped_query_type_,
                                  state.rows_affected_);
    state.skipped_stmt_ = false;
    return Transition::PROCEED;
  }

  auto portal_itr = interpreter.portals_.find(portal_name);
  if (portal_itr == interpreter.portals_.end())
    throw NetworkProcessException("Did not find portal: " + portal_name);

  std::shared_ptr<Portal> portal = portal_itr->second;
  state.statement_ = portal->GetStatement();

  if (state.statement_ == nullptr)
    throw NetworkProcessException(
        "Did not find statement in portal: " + portal_name);

  state.param_values_ = portal->GetParameters();
  auto status = tcop::Tcop::GetInstance().ExecuteStatement(state, callback);
  if (state.is_queuing_) return Transition::NEED_RESULT;
  interpreter.ExecExecuteMessageGetResult(out, status);
  return Transition::PROCEED;
}

Transition SyncCommand::Exec(PostgresProtocolInterpreter &interpreter,
                             PostgresPacketWriter &out,
                             CallbackFunc) {
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  out.WriteReadyForQuery(state.txn_state_);
  return Transition::PROCEED;
}

Transition CloseCommand::Exec(PostgresProtocolInterpreter &interpreter,
                              PostgresPacketWriter &out,
                              CallbackFunc) {
  tcop::ClientProcessState &state = interpreter.ClientProcessState();
  auto close_type = in_.ReadValue<PostgresNetworkObjectType>();
  std::string name = in_.ReadString();
  switch (close_type) {
    case PostgresNetworkObjectType::STATEMENT: {
      LOG_TRACE("Deleting statement %s from cache", name.c_str());
      state.statement_cache_.DeleteStatement(name);
      break;
    }
    case PostgresNetworkObjectType::PORTAL: {
      LOG_TRACE("Deleting portal %s from cache", name.c_str());
      auto portal_itr = interpreter.portals_.find(name);
      if (portal_itr != interpreter.portals_.end())
        // delete portal if it exists
        interpreter.portals_.erase(portal_itr);
      break;
    }
    default:
      // do nothing, simply send close complete
      break;
  }
  // Send close complete response
  out.WriteSingleTypePacket(NetworkMessageType::CLOSE_COMPLETE);
  return Transition::PROCEED;
}

Transition TerminateCommand::Exec(PostgresProtocolInterpreter &,
                                  PostgresPacketWriter &,
                                  CallbackFunc) {
  return Transition::TERMINATE;
}
} // namespace network
} // namespace peloton