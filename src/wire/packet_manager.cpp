//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// protocol.cpp
//
// Identification: src/wire/protocol.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "wire/packet_manager.h"

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
#include "tcop/tcop.h"
#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "wire/marshal.h"

#define SSL_MESSAGE_VERNO 80877103
#define PROTO_MAJOR_VERSION(x) x >> 16
#define UNUSED(x) (void)(x)

namespace peloton {
namespace wire {

// TODO: Remove hardcoded auth strings
// Hardcoded authentication strings used during session startup. To be removed
const std::unordered_map<std::string, std::string>
    // clang-format off
    PacketManager::parameter_status_map_ =
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

std::vector<PacketManager *> PacketManager::packet_managers_;
std::mutex PacketManager::packet_managers_mutex_;

PacketManager::PacketManager()
    : txn_state_(NetworkTransactionStateType::IDLE), pkt_cntr_(0) {
  traffic_cop_.reset(new tcop::TrafficCop());
  {
    std::lock_guard<std::mutex> lock(PacketManager::packet_managers_mutex_);
    PacketManager::packet_managers_.push_back(this);
    LOG_DEBUG("Registered new PacketManager [count=%d]",
              (int)PacketManager::packet_managers_.size());
  }
}

PacketManager::~PacketManager() {
  {
    std::lock_guard<std::mutex> lock(PacketManager::packet_managers_mutex_);
    PacketManager::packet_managers_.erase(
        std::remove(PacketManager::packet_managers_.begin(),
                    PacketManager::packet_managers_.end(), this),
        PacketManager::packet_managers_.end());
    LOG_DEBUG("Removed PacketManager [count=%d]",
              (int)PacketManager::packet_managers_.size());
  }
}

void PacketManager::InvalidatePreparedStatements(oid_t table_id) {
  if (table_statement_cache_.find(table_id) == table_statement_cache_.end()) {
    return;
  }
  LOG_DEBUG("Marking all PreparedStatements that access table '%d' as invalid",
            (int)table_id);
  for (auto statement : table_statement_cache_[table_id]) {
    LOG_DEBUG("Setting PreparedStatement '%s' as needing to be replanned",
              statement->GetStatementName().c_str());
    statement->SetNeedsPlan(true);
  }
}

void PacketManager::ReplanPreparedStatement(Statement *statement) {
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

void PacketManager::MakeHardcodedParameterStatus(
    const std::pair<std::string, std::string> &kv) {
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = NetworkMessageType::PARAMETER_STATUS;
  PacketPutString(response.get(), kv.first);
  PacketPutString(response.get(), kv.second);
  responses.push_back(std::move(response));
}

/*
 * process_startup_packet - Processes the startup packet
 *  (after the size field of the header).
 */
int PacketManager::ProcessInitialPacket(InputPacket *pkt) {
  std::string token, value;
  std::unique_ptr<OutputPacket> response(new OutputPacket());

  int32_t proto_version = PacketGetInt(pkt, sizeof(int32_t));
  LOG_INFO("protocol version: %d", proto_version);
  bool res;
  int res_base = 0;
  // TODO: consider more about return value
  if (proto_version == SSL_MESSAGE_VERNO) {
    res = ProcessSSLRequestPacket(pkt);
    if (!res)
      res_base = 0;
    else
      res_base = -1;
  }
  else {
    res = ProcessStartupPacket(pkt, proto_version);
    if (!res)
      res_base = 0;
    else
      res_base = 1;
  }

  return res_base;
}

bool PacketManager::ProcessStartupPacket(InputPacket* pkt, int32_t proto_version) {
  std::string token, value;
  std::unique_ptr<OutputPacket> response(new OutputPacket());

  // Only protocol version 3 is supported
  if (PROTO_MAJOR_VERSION(proto_version) != 3) {
    LOG_ERROR("Protocol error: Only protocol version 3 is supported.");
    exit(EXIT_FAILURE);
  }

  // TODO: check for more malformed cases
  // iterate till the end
  for (;;) {
    // loop end case?
    if (pkt->ptr >= pkt->len) break;
    GetStringToken(pkt, token);

    // if the option database was found
    if (token.compare("database") == 0) {
      // loop end?
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, client_.dbname);
    } else if (token.compare(("user")) == 0) {
      // loop end?
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, client_.user);
    } else {
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, value);
      client_.cmdline_options[token] = value;
    }
  }

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
  force_flush = true;

  return true;
}

bool PacketManager::ProcessSSLRequestPacket(InputPacket *pkt) {
  UNUSED(pkt);
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  // TODO: consider more about a proper response
  response->msg_type = NetworkMessageType::SSL_YES;
  responses.push_back(std::move(response));
  force_flush = true;
  return true;
}

void PacketManager::PutTupleDescriptor(
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

void PacketManager::SendDataRows(std::vector<StatementResult> &results,
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

void PacketManager::CompleteCommand(const std::string &query, const QueryType& query_type, int rows) {
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
     case QueryType::QUERY_CREATE:
      tag += " TABLE";
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
void PacketManager::SendEmptyQueryResponse() {
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = NetworkMessageType::EMPTY_QUERY_RESPONSE;
  responses.push_back(std::move(response));
}

bool PacketManager::HardcodedExecuteFilter(QueryType query_type) {
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
void PacketManager::ExecQueryMessage(InputPacket *pkt, const size_t thread_id) {
  std::string query;
  PacketGetString(pkt, pkt->len, query);

  // pop out the last character if it is ';'
  if (query.back() == ';') {
    query.pop_back();
  }
  boost::trim(query);

  if (!query.empty()) {
    std::vector<StatementResult> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_affected = 0;

    std::string query_type_string_;
    Statement::ParseQueryTypeString(query, query_type_string_);

    QueryType query_type;
    Statement::MapToQueryType(query_type_string_, query_type);
    std::stringstream stream(query_type_string_);

    switch (query_type) {
      case QueryType::QUERY_PREPARE:
      {
        std::string statement_name;
        stream >> statement_name;
        std::size_t pos = query.find("AS");
        std::string statement_query = query.substr(pos + 3);
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
          return;
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
        std::shared_ptr<Statement> statement;
        std::vector<type::Value> param_values;
        bool unnamed = false;
        std::vector<std::string> tokens;

        boost::split(tokens, query, boost::is_any_of("(), "));

        statement_name = tokens.at(1);
        auto statement_cache_itr = statement_cache_.find(statement_name);
        if (statement_cache_itr != statement_cache_.end()) {
          statement = *statement_cache_itr;
        }
        // Did not find statement with same name
        else {
          std::string error_message = "The prepared statement does not exist";
          LOG_ERROR("%s", error_message.c_str());
          SendErrorResponse(
              {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
          SendReadyForQuery(NetworkTransactionStateType::IDLE);
          return;
        }

        std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

        for (std::size_t idx = 2; idx < tokens.size(); idx++) {
          std::string param_str = tokens.at(idx);
          boost::trim(param_str);
          if (param_str.empty()) {
            continue;
          }
          param_values.push_back(type::ValueFactory::GetVarcharValue(param_str));
        }

        if (param_values.size() > 0) {
          statement->GetPlanTree()->SetParameterValues(&param_values);
        }

        auto status =
                traffic_cop_->ExecuteStatement(statement, param_values, unnamed, nullptr, result_format,
                             result, rows_affected, error_message, thread_id);

        if (status == ResultType::SUCCESS) {
          tuple_descriptor = statement->GetTupleDescriptor();
        } else {
          SendErrorResponse(
                {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
          SendReadyForQuery(NetworkTransactionStateType::IDLE);
          return;
        }
      	break;
      }
      default:
      {
        // execute the query using tcop
        auto status = traffic_cop_->ExecuteStatement(
            query, result, tuple_descriptor, rows_affected, error_message,
            thread_id);

      // check status
        if (status == ResultType::FAILURE) {
          SendErrorResponse(
            {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
          SendReadyForQuery(NetworkTransactionStateType::IDLE);
          return;
        }
      }
    }

    // send the attribute names
    PutTupleDescriptor(tuple_descriptor);

    // send the result rows
    SendDataRows(result, tuple_descriptor.size(), rows_affected);

    // The response to the SimpleQueryCommand is the query string.
    CompleteCommand(query, query_type, rows_affected);
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
}

/*
 * exec_parse_message - handle PARSE message
 */
void PacketManager::ExecParseMessage(InputPacket *pkt) {
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
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
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

void PacketManager::ExecBindMessage(InputPacket *pkt) {
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
  std::vector<std::pair<int, std::string>> bind_parameters(num_params);
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
  if (FLAGS_stats_mode != STATS_TYPE_INVALID && num_params > 0) {
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

size_t PacketManager::ReadParamType(InputPacket *pkt, int num_params,
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

size_t PacketManager::ReadParamFormat(InputPacket *pkt, int num_params_format,
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
size_t PacketManager::ReadParamValue(
    InputPacket *pkt, int num_params, std::vector<int32_t> &param_types,
    std::vector<std::pair<int, std::string>> &bind_parameters,
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
            bind_parameters[param_idx] = std::make_pair(
                type::TypeId::VARBINARY,
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

bool PacketManager::ExecDescribeMessage(InputPacket *pkt) {
  if (skipped_stmt_) {
    // send 'no-data' message
    std::unique_ptr<OutputPacket> response(new OutputPacket());
    response->msg_type = NetworkMessageType::NO_DATA_RESPONSE;
    responses.push_back(std::move(response));
    return true;
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
      return false;
    }

    auto portal = portal_itr->second;
    if (portal == nullptr) {
      LOG_ERROR("Portal does not exist : %s", portal_name.c_str());
      std::vector<FieldInfo> tuple_descriptor;
      PutTupleDescriptor(tuple_descriptor);
      return false;
    }

    auto statement = portal->GetStatement();
    PutTupleDescriptor(statement->GetTupleDescriptor());
  } else {
    LOG_TRACE("Describe a prepared statement");
  }
  return true;
}

void PacketManager::ExecExecuteMessage(InputPacket *pkt,
                                       const size_t thread_id) {
  // EXECUTE message
  std::vector<StatementResult> results;
  std::string error_message, portal_name;
  int rows_affected = 0;
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
      CompleteCommand(skipped_query_type_string, skipped_query_type_, rows_affected);
    }
    skipped_stmt_ = false;
    return;
  }

  auto portal = portals_[portal_name];
  if (portal.get() == nullptr) {
    LOG_ERROR("Did not find portal : %s", portal_name.c_str());
    SendErrorResponse(
        {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
    SendReadyForQuery(txn_state_);
    return;
  }

  auto statement = portal->GetStatement();
  const auto &query_type = statement->GetQueryType();

  auto param_stat = portal->GetParamStat();
  if (statement.get() == nullptr) {
    LOG_ERROR("Did not find statement in portal : %s", portal_name.c_str());
    SendErrorResponse(
        {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
    SendReadyForQuery(txn_state_);
    return;
  }

  auto statement_name = statement->GetStatementName();
  bool unnamed = statement_name.empty();
  auto param_values = portal->GetParameters();

  auto status = traffic_cop_->ExecuteStatement(
      statement, param_values, unnamed, param_stat, result_format_, results,
      rows_affected, error_message, thread_id);

  switch (status) {
    case ResultType::FAILURE:
      LOG_ERROR("Failed to execute: %s", error_message.c_str());
      SendErrorResponse(
          {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
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
      auto tuple_descriptor = statement->GetTupleDescriptor();
      SendDataRows(results, tuple_descriptor.size(), rows_affected);
      // The reponse to ExecuteCommand is the query_type string token.
      CompleteCommand(statement->GetQueryTypeString(), query_type, rows_affected);
      return;
    }
  }
}

void PacketManager::ExecCloseMessage(InputPacket *pkt) {
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

/*
 * process_packet - Main switch block; process incoming packets,
 *  Returns false if the session needs to be closed.
 */
bool PacketManager::ProcessPacket(InputPacket *pkt, const size_t thread_id) {
  LOG_TRACE("Message type: %c", static_cast<unsigned char>(pkt->msg_type));
  // We don't set force_flush to true for `PBDE` messages because they're
  // part of the extended protocol. Buffer responses and don't flush until
  // we see a SYNC
  switch (pkt->msg_type) {
    case NetworkMessageType::SIMPLE_QUERY_COMMAND: {
      LOG_TRACE("SIMPLE_QUERY_COMMAND");
      ExecQueryMessage(pkt, thread_id);
      force_flush = true;
    } break;
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
    } break;
    case NetworkMessageType::EXECUTE_COMMAND: {
      LOG_TRACE("EXECUTE_COMMAND");
      ExecExecuteMessage(pkt, thread_id);
    } break;
    case NetworkMessageType::SYNC_COMMAND: {
      LOG_TRACE("SYNC_COMMAND");
      SendReadyForQuery(txn_state_);
      force_flush = true;
    } break;
    case NetworkMessageType::CLOSE_COMMAND: {
      LOG_TRACE("CLOSE_COMMAND");
      ExecCloseMessage(pkt);
    } break;
    case NetworkMessageType::TERMINATE_COMMAND: {
      LOG_TRACE("TERMINATE_COMMAND");
      force_flush = true;
      return false;
    } break;
    case NetworkMessageType::NULL_COMMAND: {
      LOG_TRACE("NULL");
      force_flush = true;
      return false;
    } break;
    default: {
      LOG_ERROR("Packet type not supported yet: %d (%c)",
                static_cast<int>(pkt->msg_type),
                static_cast<unsigned char>(pkt->msg_type));
    }
  }
  return true;
}

/*
 * send_error_response - Sends the passed string as an error response.
 *    For now, it only supports the human readable 'M' message body
 */
void PacketManager::SendErrorResponse(
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

void PacketManager::SendReadyForQuery(NetworkTransactionStateType txn_status) {
  std::unique_ptr<OutputPacket> pkt(new OutputPacket());
  pkt->msg_type = NetworkMessageType::READY_FOR_QUERY;

  PacketPutByte(pkt.get(), static_cast<unsigned char>(txn_status));

  responses.push_back(std::move(pkt));
}

void PacketManager::Reset() {
  client_.Reset();
  is_started = false;
  force_flush = false;

  responses.clear();
  unnamed_statement_.reset();
  result_format_.clear();
  txn_state_ = NetworkTransactionStateType::IDLE;
  skipped_stmt_ = false;
  skipped_query_string_.clear();

  statement_cache_.clear();
  table_statement_cache_.clear();
  portals_.clear();
  pkt_cntr_ = 0;

  traffic_cop_->Reset();
}

}  // End wire namespace
}  // End peloton namespace

