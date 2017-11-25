//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_protocol_handler.cpp
//
// Identification: src/network/postgres_protocol_handler.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "network/postgres_protocol_handler.h"

#include <boost/algorithm/string.hpp>
#include <cstdio>
#include <unordered_map>

#include "common/cache.h"
#include "common/macros.h"
#include "common/portal.h"
#include "planner/abstract_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "include/traffic_cop/traffic_cop.h"
#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "network/marshal.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace network {

// TODO: Remove hardcoded auth strings
// Hardcoded authentication strings used during session startup. To be removed
const std::unordered_map<std::string, std::string>
    // clang-format off
    PostgresProtocolHandler::parameter_status_map_ =
        boost::assign::map_list_of("application_name", "psql")
				("client_encoding", "UTF8")
				("DateStyle", "ISO, MDY")
				("integer_datetimes", "on")
				("IntervalStyle", "postgres")
				("is_superuser", "on")
				("server_encoding", "UTF8")
				("server_version", "9.5devel")
				("session_authorization", "postgres")
				("standard_conforming_strings", "on")
				("TimeZone", "US/Eastern");
// clang-format on

PostgresProtocolHandler::PostgresProtocolHandler(tcop::TrafficCop *traffic_cop)
    : ProtocolHandler(traffic_cop),
      txn_state_(NetworkTransactionStateType::IDLE) {
}

PostgresProtocolHandler::~PostgresProtocolHandler() {}

// TODO: This function is used when txn cache is done
void PostgresProtocolHandler::ReplanPreparedStatement(Statement *statement) {
  std::string error_message;
  auto new_statement = traffic_cop_->PrepareStatement(
      statement->GetStatementName(), statement->GetQueryString(),
      error_message);
  // But then rip out its query plan and stick it in our old statement
  if (new_statement.get() == nullptr) {
    LOG_ERROR(
        "Failed to generate a new query plan for PreparedStatement '%s'\n%s",
        statement->GetStatementName().c_str(), error_message.c_str());
  } else {
    LOG_DEBUG("Generating new plan for PreparedStatement '%s'",
              statement->GetStatementName().c_str());

    auto old_plan = statement->GetPlanTree();
    auto new_plan = new_statement->GetPlanTree();
    statement->SetPlanTree(new_plan);
    new_statement->SetPlanTree(old_plan);
    statement->SetNeedsPlan(false);

    // TODO: We may need to delete the old plan and new statement here
  }
}

void PostgresProtocolHandler::SendInitialResponse() {
  std::unique_ptr<OutputPacket> response(new OutputPacket());

  // send auth-ok ('R')
  response->msg_type = NetworkMessageType::AUTHENTICATION_REQUEST;
  PacketPutInt(response.get(), 0, 4);
  responses.push_back(std::move(response));

  // Send the parameterStatus map ('S')
  for (auto it = parameter_status_map_.begin();
       it != parameter_status_map_.end(); it++) {
    MakeHardcodedParameterStatus(*it);
  }

  // ready-for-query packet -> 'Z'
  SendReadyForQuery(NetworkTransactionStateType::IDLE);

  // we need to send the response right away
  SetFlushFlag(true);
}

void PostgresProtocolHandler::MakeHardcodedParameterStatus(
    const std::pair<std::string, std::string> &kv) {
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = NetworkMessageType::PARAMETER_STATUS;
  PacketPutString(response.get(), kv.first);
  PacketPutString(response.get(), kv.second);
  responses.push_back(std::move(response));
}


void PostgresProtocolHandler::PutTupleDescriptor(
    const std::vector<FieldInfo> &tuple_descriptor) {
  if (tuple_descriptor.empty()) return;

  std::unique_ptr<OutputPacket> pkt(new OutputPacket());
  pkt->msg_type = NetworkMessageType::ROW_DESCRIPTION;
  PacketPutInt(pkt.get(), tuple_descriptor.size(), 2);

  for (auto col : tuple_descriptor) {
    PacketPutString(pkt.get(), std::get<0>(col));
    // TODO: Table Oid (int32)
    PacketPutInt(pkt.get(), 0, 4);
    // TODO: Attr id of column (int16)
    PacketPutInt(pkt.get(), 0, 2);
    // Field data type (int32)
    PacketPutInt(pkt.get(), std::get<1>(col), 4);
    // Data type size (int16)
    PacketPutInt(pkt.get(), std::get<2>(col), 2);
    // Type modifier (int32)
    PacketPutInt(pkt.get(), -1, 4);
    // Format code for text
    PacketPutInt(pkt.get(), 0, 2);
  }
  responses.push_back(std::move(pkt));
}

void PostgresProtocolHandler::SendDataRows(std::vector<StatementResult> &results,
                                 int colcount, int &rows_affected) {
  if (results.empty() || colcount == 0) return;

  size_t numrows = results.size() / colcount;

  // 1 packet per row
  for (size_t i = 0; i < numrows; i++) {
    std::unique_ptr<OutputPacket> pkt(new OutputPacket());
    pkt->msg_type = NetworkMessageType::DATA_ROW;
    PacketPutInt(pkt.get(), colcount, 2);
    for (int j = 0; j < colcount; j++) {
      auto content = results[i * colcount + j].second;
      if (content.size() == 0) {
        // content is NULL
        PacketPutInt(pkt.get(), NULL_CONTENT_SIZE, 4);
        // no value bytes follow
      } else {
        // length of the row attribute
        PacketPutInt(pkt.get(), content.size(), 4);
        // contents of the row attribute
        PacketPutBytes(pkt.get(), content);
      }
    }
    responses.push_back(std::move(pkt));
  }
  rows_affected = numrows;
}

void PostgresProtocolHandler::CompleteCommand(const std::string &query, const QueryType& query_type, int rows) {
  std::unique_ptr<OutputPacket> pkt(new OutputPacket());
  pkt->msg_type = NetworkMessageType::COMMAND_COMPLETE;
  std::string query_type_string;
  Statement::ParseQueryTypeString(query, query_type_string);
  std::string tag = query_type_string ;
  switch (query_type) {
    /* After Begin, we enter a txn block */
    case QueryType::QUERY_BEGIN:
      txn_state_ = NetworkTransactionStateType::BLOCK;
      break;
    /* After commit, we end the txn block */
    case QueryType::QUERY_COMMIT:
    /* After rollback, the txn block is ended */
    case QueryType::QUERY_ROLLBACK:
      txn_state_ = NetworkTransactionStateType::IDLE;
      break;
    case QueryType::QUERY_INSERT:
      tag += " 0 " + std::to_string(rows);
      break;
    case QueryType::QUERY_CREATE: {
      std::string create_type_string;
      Statement::ParseCreateTypeString(query, create_type_string);
      tag += " " + create_type_string;
      break;
    }
    case QueryType::QUERY_PREPARE:
      break;
    default:
      tag += " " + std::to_string(rows);
   }
   PacketPutString(pkt.get(), tag);
   responses.push_back(std::move(pkt));
}

/*
 * put_empty_query_response - Informs the client that an empty query was sent
 */
void PostgresProtocolHandler::SendEmptyQueryResponse() {
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = NetworkMessageType::EMPTY_QUERY_RESPONSE;
  responses.push_back(std::move(response));
}

bool PostgresProtocolHandler::HardcodedExecuteFilter(QueryType query_type) {
  switch (query_type) {
    // Skip SET
    case QueryType::QUERY_SET:
    case QueryType::QUERY_SHOW:
      return false;
    // Skip duplicate BEGIN
    case QueryType::QUERY_BEGIN:
      if (txn_state_ == NetworkTransactionStateType::BLOCK) {
        return false;
      }
      break;
    // Skip duuplicate Commits and Rollbacks
    case QueryType::QUERY_COMMIT:
    case QueryType::QUERY_ROLLBACK:
      if (txn_state_ == NetworkTransactionStateType::IDLE) {
        return false;
      }
    default:
      break;
  }
  return true;
}

// The Simple Query Protocol
// Fix mis-split bug: Previously, this function assumes there are multiple
// queries in the string and split it by ';', which would cause one containing
// ';' being split into multiple queries.
// However, the multi-statement queries has been split by the psql client and
// there is no need to split the query again.
ProcessResult PostgresProtocolHandler::ExecQueryMessage(InputPacket *pkt, const size_t thread_id) {
  std::string query;
  PacketGetString(pkt, pkt->len, query);

  // pop out the last character if it is ';'
  if (query.back() == ';') {
    query.pop_back();
  }
  boost::trim(query);

  protocol_type_ = NetworkProtocolType::POSTGRES_PSQL;

  if (!query.empty()) {
    std::vector<StatementResult> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_affected = 0;

    std::string query_type_string_;
    Statement::ParseQueryTypeString(query, query_type_string_);

    QueryType query_type;
    Statement::MapToQueryType(query_type_string_, query_type);

    query_ = query;
    query_type_ = query_type;

    switch (query_type) {
      case QueryType::QUERY_PREPARE:
      {
        std::string statement_name;
        std::vector<std::string> tokens;
        boost::split(tokens, query_, boost::is_any_of("(), "));
        statement_name = tokens.at(1);
        std::size_t pos = boost::to_upper_copy(query_).find("AS");
        std::string statement_query = query_.substr(pos + 3);
        boost::trim(statement_query);

        // Prepare statement
        std::shared_ptr<Statement> statement(nullptr);

        LOG_DEBUG("PrepareStatement[%s] => %s", statement_name.c_str(),
                statement_query.c_str());

        statement = traffic_cop_->PrepareStatement(statement_name, statement_query,
                                                 error_message);
        if (statement.get() == nullptr) {
          skipped_stmt_ = true;
          SendErrorResponse(
              {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
          LOG_TRACE("ExecQuery Error");
          SendReadyForQuery(NetworkTransactionStateType::IDLE);
          return ProcessResult::COMPLETE;
        }

        auto entry = std::make_pair(statement_name, statement);
        statement_cache_.insert(entry);
        for (auto table_id : statement->GetReferencedTables()) {
          table_statement_cache_[table_id].push_back(statement.get());
        }
        break;
      }
      case QueryType::QUERY_EXECUTE:
      {
        std::string statement_name;
        std::vector<type::Value> param_values;
        bool unnamed = false;
        std::vector<std::string> tokens;

        boost::split(tokens, query, boost::is_any_of("(), "));

        statement_name = tokens.at(1);
        auto statement_cache_itr = statement_cache_.find(statement_name);
        if (statement_cache_itr != statement_cache_.end()) {
          statement_ = *statement_cache_itr;
        }
        // Did not find statement with same name
        else {
          error_message_ = "The prepared statement does not exist";
          LOG_ERROR("%s", error_message_.c_str());
          SendErrorResponse(
              {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message_}});
          SendReadyForQuery(NetworkTransactionStateType::IDLE);
          return ProcessResult::COMPLETE;
        }

        query_type_ = statement_->GetQueryType();
        query_ = statement_->GetQueryString();
        std::vector<int> result_format(statement_->GetTupleDescriptor().size(), 0);
        result_format_ = result_format;

        for (std::size_t idx = 2; idx < tokens.size(); idx++) {
          std::string param_str = tokens.at(idx);
          boost::trim(param_str);
          if (param_str.empty()) {
            continue;
          }
          param_values.push_back(type::ValueFactory::GetVarcharValue(param_str));
        }

        if (param_values.size() > 0) {
          statement_->GetPlanTree()->SetParameterValues(&param_values);
        }
        param_values_ = param_values;

        auto status =
                traffic_cop_->ExecuteStatement(statement_, param_values_, unnamed, nullptr, result_format_,
                             results_, rows_affected_, error_message_, thread_id);

        if (traffic_cop_->is_queuing_) {
          return ProcessResult::PROCESSING;
        }

        ExecQueryMessageGetResult(status);
        return ProcessResult::COMPLETE;
      }
      default:
      {
        // prepareStatement
        std::string unnamed_statement = "unnamed";
        statement_ = traffic_cop_->PrepareStatement(unnamed_statement, query_,
                                                    error_message);
        if (statement_.get() == nullptr) {
          rows_affected = 0;
          SendErrorResponse(
            {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
          SendReadyForQuery(NetworkTransactionStateType::IDLE);
          return ProcessResult::COMPLETE;
        }
        // ExecuteStatment
        std::vector<type::Value> param_values;
        param_values_ = param_values;
        bool unnamed = false;
        std::vector<int> result_format(statement_->GetTupleDescriptor().size(), 0);
        result_format_ = result_format;
        // should param_values and result_format be local variable?
        // should results_ be reset when PakcetManager.reset(), why results_ cannot be read?
        auto status =
            traffic_cop_->ExecuteStatement(statement_, param_values_, unnamed, nullptr, result_format_,
                                           results_, rows_affected_, error_message_, thread_id);
        if (traffic_cop_->is_queuing_) {
          return ProcessResult::PROCESSING;
        }
        ExecQueryMessageGetResult(status);
        return ProcessResult::COMPLETE;
      }
    }

    // send the attribute names
    PutTupleDescriptor(tuple_descriptor);

    // send the result rows
    SendDataRows(result, tuple_descriptor.size(), rows_affected);

    // The response to the SimpleQueryCommand is the query string.
    CompleteCommand(query_, query_type_, rows_affected);
  } else {
    SendEmptyQueryResponse();
  }

  // PAVLO: 2017-01-15
  // There used to be code here that would invoke this method passing
  // in NetworkMessageType::READY_FOR_QUERY as the argument. But when
  // I switched to strong types, this obviously doesn't work. So I
  // switched it to be NetworkTransactionStateType::IDLE. I don't know
  // we just don't always send back the internal txn state?
  SendReadyForQuery(NetworkTransactionStateType::IDLE);
  return ProcessResult::COMPLETE;
}

void PostgresProtocolHandler::ExecQueryMessageGetResult(ResultType status) {
  std::vector<FieldInfo> tuple_descriptor;
  if (status == ResultType::SUCCESS) {
    tuple_descriptor = statement_->GetTupleDescriptor();
  } else if (status == ResultType::FAILURE) { // check status
    SendErrorResponse(
        {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message_}});
    SendReadyForQuery(NetworkTransactionStateType::IDLE);
    return;
  }

  // send the attribute names
  PutTupleDescriptor(tuple_descriptor);

  // send the result rows
  SendDataRows(results_, tuple_descriptor.size(), rows_affected_);

  // The response to the SimpleQueryCommand is the query string.
  CompleteCommand(query_, query_type_, rows_affected_);

  SendReadyForQuery(NetworkTransactionStateType::IDLE);
}

/*
 * exec_parse_message - handle PARSE message
 */
void PostgresProtocolHandler::ExecParseMessage(InputPacket *pkt) {
  std::string error_message, statement_name, query_string, query_type_string;
  GetStringToken(pkt, statement_name);
  QueryType query_type;

  // Read prepare statement name
  // Read query string
  GetStringToken(pkt, query_string);
  skipped_stmt_ = false;
  Statement::ParseQueryTypeString(query_string, query_type_string);
  Statement::MapToQueryType(query_type_string, query_type);
  // For an empty query or a query to be filtered, just send parse complete
  // response and don't execute
  if (query_string == "" || HardcodedExecuteFilter(query_type) == false) {
    skipped_stmt_ = true;
    skipped_query_string_ = std::move(query_string);
    skipped_query_type_ = std::move(query_type);

    // Send Parse complete response
    std::unique_ptr<OutputPacket> response(new OutputPacket());
    response->msg_type = NetworkMessageType::PARSE_COMPLETE;
    responses.push_back(std::move(response));
    return;
  }

  // Prepare statement
  std::shared_ptr<Statement> statement(nullptr);

  LOG_DEBUG("PrepareStatement[%s] => %s", statement_name.c_str(),
            query_string.c_str());
  statement = traffic_cop_->PrepareStatement(statement_name, query_string,
                                             error_message);
  if (statement.get() == nullptr) {
    skipped_stmt_ = true;
    SendErrorResponse(
        {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
    LOG_TRACE("ExecParse Error");
    return;
  }

  // Read number of params
  int num_params = PacketGetInt(pkt, 2);

  // Read param types
  std::vector<int32_t> param_types(num_params);
  auto type_buf_begin = pkt->Begin() + pkt->ptr;
  auto type_buf_len = ReadParamType(pkt, num_params, param_types);

  // Cache the received query
  bool unnamed_query = statement_name.empty();
  statement->SetParamTypes(param_types);

  // Stat
  if (settings::SettingsManager::GetInt(settings::SettingId::stats_mode) != STATS_TYPE_INVALID) {
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

  // Unnamed statement
  if (unnamed_query) {
    unnamed_statement_ = statement;
  } else {
    auto entry = std::make_pair(statement_name, statement);
    statement_cache_.insert(entry);
    for (auto table_id : statement->GetReferencedTables()) {
      table_statement_cache_[table_id].push_back(statement.get());
    }
  }
  // Send Parse complete response
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = NetworkMessageType::PARSE_COMPLETE;
  responses.push_back(std::move(response));
}

void PostgresProtocolHandler::ExecBindMessage(InputPacket *pkt) {
  std::string portal_name, statement_name;
  // BIND message
  GetStringToken(pkt, portal_name);
  GetStringToken(pkt, statement_name);

  if (skipped_stmt_) {
    // send bind complete
    std::unique_ptr<OutputPacket> response(new OutputPacket());
    response->msg_type = NetworkMessageType::BIND_COMPLETE;
    responses.push_back(std::move(response));
    return;
  }

  // Read parameter format
  int num_params_format = PacketGetInt(pkt, 2);
  std::vector<int16_t> formats(num_params_format);

  auto format_buf_begin = pkt->Begin() + pkt->ptr;
  auto format_buf_len = ReadParamFormat(pkt, num_params_format, formats);

  int num_params = PacketGetInt(pkt, 2);
  // error handling
  if (num_params_format != num_params) {
    std::string error_message =
        "Malformed request: num_params_format is not equal to num_params";
    SendErrorResponse(
        {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
    return;
  }

  // Get statement info generated in PARSE message
  std::shared_ptr<Statement> statement;
  stats::QueryMetric::QueryParamBuf param_type_buf;

  // UNNAMED STATEMENT
  if (statement_name.empty()) {
    statement = unnamed_statement_;
    param_type_buf = unnamed_stmt_param_types_;

    // Check unnamed statement
    if (statement.get() == nullptr) {
      std::string error_message = "Invalid unnamed statement";
      LOG_ERROR("%s", error_message.c_str());
      SendErrorResponse(
          {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
      return;
    }
    // NAMED STATEMENT
  } else {
    auto statement_cache_itr = statement_cache_.find(statement_name);
    if (statement_cache_itr != statement_cache_.end()) {
      statement = *statement_cache_itr;
      param_type_buf = statement_param_types_[statement_name];
    }
    // Did not find statement with same name
    else {
      std::string error_message = "The prepared statement does not exist";
      LOG_ERROR("%s", error_message.c_str());
      SendErrorResponse(
          {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
      return;
    }
  }

  const auto &query_string = statement->GetQueryString();
  const auto &query_type = statement->GetQueryType();

  // check if the loaded statement needs to be skipped
  skipped_stmt_ = false;
  if (HardcodedExecuteFilter(query_type) == false) {
    skipped_stmt_ = true;
    skipped_query_string_ = query_string;
    std::unique_ptr<OutputPacket> response(new OutputPacket());
    // Send Bind complete response
    response->msg_type = NetworkMessageType::BIND_COMPLETE;
    responses.push_back(std::move(response));
    return;
  }

  // Check whether somebody wants us to generate a new query plan
  // for this prepared statement
  if (statement->GetNeedsPlan()) {
    ReplanPreparedStatement(statement.get());
  }

  // Group the parameter types and the parameters in this vector
  std::vector<std::pair<type::TypeId, std::string>> bind_parameters(num_params);
  std::vector<type::Value> param_values(num_params);

  auto param_types = statement->GetParamTypes();

  auto val_buf_begin = pkt->Begin() + pkt->ptr;
  auto val_buf_len = ReadParamValue(pkt, num_params, param_types,
                                    bind_parameters, param_values, formats);

  int format_codes_number = PacketGetInt(pkt, 2);
  LOG_TRACE("format_codes_number: %d", format_codes_number);
  // Set the result-column format code
  if (format_codes_number == 0) {
    // using the default text format
    result_format_ =
        std::vector<int>(statement->GetTupleDescriptor().size(), 0);
  } else if (format_codes_number == 1) {
    // get the format code from packet
    auto result_format = PacketGetInt(pkt, 2);
    result_format_ = std::vector<int>(
        statement->GetTupleDescriptor().size(), result_format);
  } else {
    // get the format code for each column
    result_format_.clear();
    for (int format_code_idx = 0; format_code_idx < format_codes_number;
         ++format_code_idx) {
      result_format_.push_back(PacketGetInt(pkt, 2));
      LOG_TRACE("format code: %d", *result_format_.rbegin());
    }
  }

  if (param_values.size() > 0) {
    statement->GetPlanTree()->SetParameterValues(&param_values);
    // Instead of tree traversal, we should put param values in the
    // executor context.
  }

  std::shared_ptr<stats::QueryMetric::QueryParams> param_stat(nullptr);
  if (settings::SettingsManager::GetInt(settings::SettingId::stats_mode) != STATS_TYPE_INVALID
      && num_params > 0) {
    // Make a copy of format for stat collection
    stats::QueryMetric::QueryParamBuf param_format_buf;
    param_format_buf.len = format_buf_len;
    param_format_buf.buf = PacketCopyBytes(format_buf_begin, format_buf_len);
    PL_ASSERT(format_buf_len > 0);

    // Make a copy of value for stat collection
    stats::QueryMetric::QueryParamBuf param_val_buf;
    param_val_buf.len = val_buf_len;
    param_val_buf.buf = PacketCopyBytes(val_buf_begin, val_buf_len);
    PL_ASSERT(val_buf_len > 0);

    param_stat.reset(new stats::QueryMetric::QueryParams(
        param_format_buf, param_type_buf, param_val_buf, num_params));
  }

  // Construct a portal.
  // Notice that this will move param_values so no value will be left there.
  auto portal =
      new Portal(portal_name, statement, std::move(param_values), param_stat);
  std::shared_ptr<Portal> portal_reference(portal);

  auto itr = portals_.find(portal_name);
  // Found portal name in portal map
  if (itr != portals_.end()) {
    itr->second = portal_reference;
  }
  // Create a new entry in portal map
  else {
    portals_.insert(std::make_pair(portal_name, portal_reference));
  }
  // send bind complete
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = NetworkMessageType::BIND_COMPLETE;
  responses.push_back(std::move(response));
}

size_t PostgresProtocolHandler::ReadParamType(InputPacket *pkt, int num_params,
                                    std::vector<int32_t> &param_types) {
  auto begin = pkt->ptr;
  // get the type of each parameter
  for (int i = 0; i < num_params; i++) {
    int param_type = PacketGetInt(pkt, 4);
    param_types[i] = param_type;
  }
  auto end = pkt->ptr;
  return end - begin;
}

size_t PostgresProtocolHandler::ReadParamFormat(InputPacket *pkt, int num_params_format,
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
size_t PostgresProtocolHandler::ReadParamValue(
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
        PL_ASSERT(param_values[param_idx].GetTypeId() != type::TypeId::INVALID);
      } else {
        // BINARY mode
        switch (static_cast<PostgresValueType>(param_types[param_idx])) {
          case PostgresValueType::INTEGER: {
            int int_val = 0;
            for (size_t i = 0; i < sizeof(int); ++i) {
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
            PL_MEMCPY(&float_val, &buf, sizeof(double));
            bind_parameters[param_idx] =
                std::make_pair(type::TypeId::DECIMAL, std::to_string(float_val));
            param_values[param_idx] =
                type::ValueFactory::GetDecimalValue(float_val).Copy();
            break;
          }
          case PostgresValueType::VARBINARY: {
            bind_parameters[param_idx] = std::make_pair(type::TypeId::VARBINARY,
                std::string(reinterpret_cast<char *>(&param[0]), param_len));
            param_values[param_idx] = type::ValueFactory::GetVarbinaryValue(
                &param[0], param_len, true);
            break;
          }
          default: {
            LOG_ERROR("Do not support data type: %d", param_types[param_idx]);
            break;
          }
        }
        PL_ASSERT(param_values[param_idx].GetTypeId() != type::TypeId::INVALID);
      }
    }
  }
  auto end = pkt->ptr;
  return end - begin;
}

ProcessResult PostgresProtocolHandler::ExecDescribeMessage(InputPacket *pkt) {
  if (skipped_stmt_) {
    // send 'no-data' message
    std::unique_ptr<OutputPacket> response(new OutputPacket());
    response->msg_type = NetworkMessageType::NO_DATA_RESPONSE;
    responses.push_back(std::move(response));
    return ProcessResult::COMPLETE;
  }

  ByteBuf mode;
  std::string portal_name;
  PacketGetBytes(pkt, 1, mode);
  GetStringToken(pkt, portal_name);
  if (mode[0] == 'P') {
    LOG_TRACE("Describe a portal");
    auto portal_itr = portals_.find(portal_name);

    // TODO: error handling here
    // Ahmed: This is causing the continuously running thread
    // Changed the function signature to return boolean
    // when false is returned, the connection is closed
    if (portal_itr == portals_.end()) {
      LOG_ERROR("Did not find portal : %s", portal_name.c_str());
      std::vector<FieldInfo> tuple_descriptor;
      PutTupleDescriptor(tuple_descriptor);
      return ProcessResult::COMPLETE;
    }

    auto portal = portal_itr->second;
    if (portal == nullptr) {
      LOG_ERROR("Portal does not exist : %s", portal_name.c_str());
      std::vector<FieldInfo> tuple_descriptor;
      PutTupleDescriptor(tuple_descriptor);
      return ProcessResult::TERMINATE;
    }

    auto statement = portal->GetStatement();
    PutTupleDescriptor(statement->GetTupleDescriptor());
  } else {
    LOG_TRACE("Describe a prepared statement");
  }
  return ProcessResult::COMPLETE;
}

ProcessResult PostgresProtocolHandler::ExecExecuteMessage(InputPacket *pkt,
                                       const size_t thread_id) {
  // EXECUTE message
  protocol_type_ = NetworkProtocolType::POSTGRES_JDBC;
  std::string error_message, portal_name;

  GetStringToken(pkt, portal_name);

  // covers weird JDBC edge case of sending double BEGIN statements. Don't
  // execute them
  if (skipped_stmt_) {
    if (skipped_query_string_ == "") {
      SendEmptyQueryResponse();
    } else {
      std::string skipped_query_type_string;
      Statement::ParseQueryTypeString(skipped_query_string_, skipped_query_type_string);
      // The response to ExecuteCommand is the query_type string token.
      CompleteCommand(skipped_query_type_string, skipped_query_type_, rows_affected_);
    }
    skipped_stmt_ = false;
    return ProcessResult::COMPLETE;
  }

  auto portal = portals_[portal_name];
  if (portal.get() == nullptr) {
    LOG_ERROR("Did not find portal : %s", portal_name.c_str());
    SendErrorResponse(
        {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message_}});
    SendReadyForQuery(txn_state_);
    return ProcessResult::TERMINATE;
  }

  statement_ = portal->GetStatement();

  auto param_stat = portal->GetParamStat();
  if (statement_.get() == nullptr) {
    LOG_ERROR("Did not find statement in portal : %s", portal_name.c_str());
    SendErrorResponse(
        {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
    SendReadyForQuery(txn_state_);
    return ProcessResult::TERMINATE;
  }

  auto statement_name = statement_->GetStatementName();
  bool unnamed = statement_name.empty();
  param_values_ = portal->GetParameters();

  auto status = traffic_cop_->ExecuteStatement(
      statement_, param_values_, unnamed, param_stat, result_format_, results_,
      rows_affected_, error_message_, thread_id);
  if (traffic_cop_->is_queuing_) {
    return ProcessResult::PROCESSING;
  }
  ExecExecuteMessageGetResult(status);
  return ProcessResult::COMPLETE;
}

void PostgresProtocolHandler::ExecExecuteMessageGetResult(ResultType status) {
  const auto &query_type = statement_->GetQueryType();
  switch (status) {
    case ResultType::FAILURE:
      LOG_ERROR("Failed to execute: %s", error_message_.c_str());
      SendErrorResponse(
          {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message_}});
      return;
    case ResultType::ABORTED:
      if (query_type != QueryType::QUERY_ROLLBACK) {
        LOG_DEBUG("Failed to execute: Conflicting txn aborted");
        // Send an error response if the abort is not due to ROLLBACK query
        SendErrorResponse({{NetworkMessageType::SQLSTATE_CODE_ERROR,
                            SqlStateErrorCodeToString(
                                SqlStateErrorCode::SERIALIZATION_ERROR)}});
      }
      return;
    default: {
      auto tuple_descriptor = statement_->GetTupleDescriptor();
      SendDataRows(results_, tuple_descriptor.size(), rows_affected_);
      // The reponse to ExecuteCommand is the query_type string token.
      CompleteCommand(statement_->GetQueryTypeString(), query_type, rows_affected_);
      return;
    }
  }
}

void PostgresProtocolHandler::GetResult() {
  traffic_cop_->ExecuteStatementPlanGetResult();
  auto status = traffic_cop_->ExecuteStatementGetResult(rows_affected_);
  switch (protocol_type_) {
    case NetworkProtocolType::POSTGRES_JDBC:
      LOG_TRACE("JDBC result");
      ExecExecuteMessageGetResult(status);
      break;
    case NetworkProtocolType::POSTGRES_PSQL:
      LOG_TRACE("PSQL result");
      ExecQueryMessageGetResult(status);
  }
}

void PostgresProtocolHandler::ExecCloseMessage(InputPacket *pkt) {
  uchar close_type = 0;
  std::string name;
  PacketGetByte(pkt, close_type);
  PacketGetString(pkt, 0, name);
  bool is_unnamed = (name.size() == 0) ? true : false;
  switch (close_type) {
    case 'S':
      LOG_TRACE("Deleting statement %s from cache", name.c_str());
      if (is_unnamed) {
        unnamed_statement_.reset();
      } else {
        // TODO: Invalidate table_statement_cache!
        statement_cache_.delete_key(name);
      }
      break;
    case 'P': {
      LOG_TRACE("Deleting portal %s from cache", name.c_str());
      auto portal_itr = portals_.find(name);
      if (portal_itr != portals_.end()) {
        // delete portal if it exists
        portals_.erase(portal_itr);
      }
      break;
    }
    default:
      // do nothing, simply send close complete
      break;
  }
  // Send close complete response
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = NetworkMessageType::CLOSE_COMPLETE;
  responses.push_back(std::move(response));
}

// The function tries to do a preliminary read to fetch the size value and
// then reads the rest of the packet.
// Assume: Packet length field is always 32-bit int
bool PostgresProtocolHandler::ReadPacketHeader(Buffer& rbuf, InputPacket& rpkt) {
  // All packets other than the startup packet have a 5 bytes header
  size_t initial_read_size = sizeof(int32_t);
  // check if header bytes are available
  if (!rbuf.IsReadDataAvailable(initial_read_size + 1)) {
    // nothing more to read
    return false;
  }

  // get packet size from the header
  // Header also contains msg type
  rpkt.msg_type =
      static_cast<NetworkMessageType>(rbuf.GetByte(rbuf.buf_ptr));
  // Skip the message type byte
  rbuf.buf_ptr++;

  // extract packet contents size
  //content lengths should exclude the length bytes
  rpkt.len = rbuf.GetUInt32BigEndian() - sizeof(uint32_t);

  // do we need to use the extended buffer for this packet?
  rpkt.is_extended = (rpkt.len > rbuf.GetMaxSize());

  if (rpkt.is_extended) {
    LOG_TRACE("Using extended buffer for pkt size:%ld", rpkt.len);
    // reserve space for the extended buffer
    rpkt.ReserveExtendedBuffer();
  }

  // we have processed the data, move buffer pointer
  rbuf.buf_ptr += initial_read_size;
  rpkt.header_parsed = true;

  return true;
}

// Tries to read the contents of a single packet, returns true on success, false
// on failure.
bool PostgresProtocolHandler::ReadPacket(Buffer &rbuf, InputPacket &rpkt) {
  if (rpkt.is_extended) {
    // extended packet mode
    auto bytes_available = rbuf.buf_size - rbuf.buf_ptr;
    auto bytes_required = rpkt.ExtendedBytesRequired();
    // read minimum of the two ranges
    auto read_size = std::min(bytes_available, bytes_required);
    rpkt.AppendToExtendedBuffer(rbuf.Begin() + rbuf.buf_ptr,
                                rbuf.Begin() + rbuf.buf_ptr + read_size);
    // data has been copied, move ptr
    rbuf.buf_ptr += read_size;
    if (bytes_required > bytes_available) {
      // more data needs to be read
      return false;
    }
    // all the data has been read
    rpkt.InitializePacket();
    return true;
  } else {
    if (rbuf.IsReadDataAvailable(rpkt.len) == false) {
      // data not available yet, return
      return false;
    }
    // Initialize the packet's "contents"
    rpkt.InitializePacket(rbuf.buf_ptr, rbuf.Begin());
    // We have processed the data, move buffer pointer
    rbuf.buf_ptr += rpkt.len;
  }

  return true;
}

ProcessResult PostgresProtocolHandler::Process(Buffer &rbuf, const size_t thread_id) {
  if (request.header_parsed == false) {
    // parse out the header first
    if (ReadPacketHeader(rbuf, request) == false) {
      // need more data
      return ProcessResult::MORE_DATA_REQUIRED;
    }
  }
  PL_ASSERT(request.header_parsed == true);

  if (request.is_initialized == false) {
    // packet needs to be initialized with rest of the contents
    if (PostgresProtocolHandler::ReadPacket(rbuf, request) == false) {
      // need more data
      return ProcessResult::MORE_DATA_REQUIRED;
    }
  }

  auto process_status = ProcessPacket(&request, thread_id);

  request.Reset();

  return process_status;
}


/*
 * process_packet - Main switch block; process incoming packets,
 *  Returns false if the session needs to be closed.
 */
ProcessResult PostgresProtocolHandler::ProcessPacket(InputPacket *pkt, const size_t thread_id) {
  LOG_TRACE("Message type: %c", static_cast<unsigned char>(pkt->msg_type));
  // We don't set force_flush to true for `PBDE` messages because they're
  // part of the extended protocol. Buffer responses and don't flush until
  // we see a SYNC
  switch (pkt->msg_type) {
    case NetworkMessageType::SIMPLE_QUERY_COMMAND: {
      LOG_TRACE("SIMPLE_QUERY_COMMAND");
      SetFlushFlag(true);
      return ExecQueryMessage(pkt, thread_id);
    }
    case NetworkMessageType::PARSE_COMMAND: {
      LOG_TRACE("PARSE_COMMAND");
      ExecParseMessage(pkt);
    } break;
    case NetworkMessageType::BIND_COMMAND: {
      LOG_TRACE("BIND_COMMAND");
      ExecBindMessage(pkt);
    } break;
    case NetworkMessageType::DESCRIBE_COMMAND: {
      LOG_TRACE("DESCRIBE_COMMAND");
      return ExecDescribeMessage(pkt);
    }
    case NetworkMessageType::EXECUTE_COMMAND: {
      LOG_TRACE("EXECUTE_COMMAND");
      return ExecExecuteMessage(pkt, thread_id);
    }
    case NetworkMessageType::SYNC_COMMAND: {
      LOG_TRACE("SYNC_COMMAND");
      SendReadyForQuery(txn_state_);
      SetFlushFlag(true);
    } break;
    case NetworkMessageType::CLOSE_COMMAND: {
      LOG_TRACE("CLOSE_COMMAND");
      ExecCloseMessage(pkt);
    } break;
    case NetworkMessageType::TERMINATE_COMMAND: {
      LOG_TRACE("TERMINATE_COMMAND");
      SetFlushFlag(true);
      return ProcessResult::TERMINATE;
    }
    case NetworkMessageType::NULL_COMMAND: {
      LOG_TRACE("NULL");
      SetFlushFlag(true);
      return ProcessResult::TERMINATE;
    }
    default: {
      LOG_ERROR("Packet type not supported yet: %d (%c)",
                static_cast<int>(pkt->msg_type),
                static_cast<unsigned char>(pkt->msg_type));
    }
  }
  return ProcessResult::COMPLETE;
}

/*
 * send_error_response - Sends the passed string as an error response.
 *    For now, it only supports the human readable 'M' message body
 */
void PostgresProtocolHandler::SendErrorResponse(
    std::vector<std::pair<NetworkMessageType, std::string>> error_status) {
  std::unique_ptr<OutputPacket> pkt(new OutputPacket());
  pkt->msg_type = NetworkMessageType::ERROR_RESPONSE;

  for (auto entry : error_status) {
    PacketPutByte(pkt.get(), static_cast<unsigned char>(entry.first));
    PacketPutString(pkt.get(), entry.second);
  }

  // put null terminator
  PacketPutByte(pkt.get(), 0);

  // don't care if write finished or not, we are closing anyway
  responses.push_back(std::move(pkt));
}

void PostgresProtocolHandler::SendReadyForQuery(NetworkTransactionStateType txn_status) {
  std::unique_ptr<OutputPacket> pkt(new OutputPacket());
  pkt->msg_type = NetworkMessageType::READY_FOR_QUERY;

  PacketPutByte(pkt.get(), static_cast<unsigned char>(txn_status));

  responses.push_back(std::move(pkt));
}

void PostgresProtocolHandler::Reset() {
  ProtocolHandler::Reset();

  unnamed_statement_.reset();
  result_format_.clear();
  results_.clear();
  param_values_.clear();
  txn_state_ = NetworkTransactionStateType::IDLE;
  skipped_stmt_ = false;
  skipped_query_string_.clear();

  statement_cache_.clear();
  table_statement_cache_.clear();
  portals_.clear();
}

}  // namespace network
}  // namespace peloton
