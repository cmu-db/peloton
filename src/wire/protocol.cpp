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

#include <cstdio>
#include <unordered_map>

#include "common/cache.h"
#include "common/types.h"
#include "common/macros.h"
#include "wire/marshal.h"
#include "common/portal.h"
#include "tcop/tcop.h"

#include "planner/abstract_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "planner/delete_plan.h"
#include "common/value.h"
#include "common/value_factory.h"

#include <boost/algorithm/string.hpp>

#define PROTO_MAJOR_VERSION(x) x >> 16

namespace peloton {
namespace wire {

// Prepares statment cache
thread_local peloton::Cache<std::string, Statement> statement_cache_;

// Query portal handler
thread_local std::unordered_map<std::string, std::shared_ptr<Portal>> portals_;

// Hardcoded authentication strings used during session startup. To be removed
const std::unordered_map<std::string, std::string>
    PacketManager::parameter_status_map =
        boost::assign::map_list_of("application_name", "psql")(
            "client_encoding", "UTF8")("DateStyle", "ISO, MDY")(
            "integer_datetimes", "on")("IntervalStyle", "postgres")(
            "is_superuser", "on")("server_encoding", "UTF8")(
            "server_version", "9.5devel")("session_authorization", "postgres")(
            "standard_conforming_strings", "on")("TimeZone", "US/Eastern");

/*
 * close_client - Close the socket of the underlying client
 */
void PacketManager::CloseClient() { client.sock->CloseSocket(); }

void PacketManager::MakeHardcodedParameterStatus(
    ResponseBuffer &responses, const std::pair<std::string, std::string> &kv) {
  std::unique_ptr<Packet> response(new Packet());
  response->msg_type = 'S';
  PacketPutString(response, kv.first);
  PacketPutString(response, kv.second);
  responses.push_back(std::move(response));
}
/*
 * process_startup_packet - Processes the startup packet
 *  (after the size field of the header).
 */
bool PacketManager::ProcessStartupPacket(Packet *pkt,
                                         ResponseBuffer &responses) {
  std::string token, value;
  std::unique_ptr<Packet> response(new Packet());

  int32_t proto_version = PacketGetInt(pkt, sizeof(int32_t));

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
      GetStringToken(pkt, client.dbname);
    } else if (token.compare(("user")) == 0) {
      // loop end?
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, client.user);
    } else {
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, value);
      client.cmdline_options[token] = value;
    }
  }

  // send auth-ok ('R')
  response->msg_type = 'R';
  PacketPutInt(response, 0, 4);
  responses.push_back(std::move(response));

  // Send the parameterStatus map ('S')
  for (auto it = parameter_status_map.begin(); it != parameter_status_map.end();
       it++) {
    MakeHardcodedParameterStatus(responses, *it);
  }

  // ready-for-query packet -> 'Z'
  SendReadyForQuery(TXN_IDLE, responses);
  return true;
}

void PacketManager::PutTupleDescriptor(
    const std::vector<FieldInfoType> &tuple_descriptor,
    ResponseBuffer &responses) {

  if (tuple_descriptor.empty()) return;

  LOG_TRACE("Put TupleDescriptor");

  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'T';
  PacketPutInt(pkt, tuple_descriptor.size(), 2);

  for (auto col : tuple_descriptor) {
    LOG_TRACE("column name: %s", std::get<0>(col).c_str());
    PacketPutString(pkt, std::get<0>(col));
    // TODO: Table Oid (int32)
    PacketPutInt(pkt, 0, 4);
    // TODO: Attr id of column (int16)
    PacketPutInt(pkt, 0, 2);
    // Field data type (int32)
    PacketPutInt(pkt, std::get<1>(col), 4);
    // Data type size (int16)
    PacketPutInt(pkt, std::get<2>(col), 2);
    // Type modifier (int32)
    PacketPutInt(pkt, -1, 4);
    // Format code for text
    PacketPutInt(pkt, 0, 2);
  }
  responses.push_back(std::move(pkt));
}

void PacketManager::SendDataRows(std::vector<ResultType> &results, int colcount,
                                 int &rows_affected,
                                 ResponseBuffer &responses) {
  if (!results.size() || !colcount) return;

  LOG_TRACE("Flatten result size: %lu", results.size());
  size_t numrows = results.size() / colcount;

  // 1 packet per row
  for (size_t i = 0; i < numrows; i++) {
    std::unique_ptr<Packet> pkt(new Packet());
    pkt->msg_type = 'D';
    PacketPutInt(pkt, colcount, 2);
    for (int j = 0; j < colcount; j++) {
      // length of the row attribute
      PacketPutInt(pkt, results[i * colcount + j].second.size(), 4);
      // contents of the row attribute
      PacketPutBytes(pkt, results[i * colcount + j].second);
    }
    responses.push_back(std::move(pkt));
  }
  rows_affected = numrows;
  LOG_TRACE("Rows affected: %d", rows_affected);
}

/* Gets the first token of a query */
std::string get_query_type(std::string query) {
  std::vector<std::string> query_tokens;
  boost::split(query_tokens, query, boost::is_any_of(" "),
               boost::token_compress_on);
  return query_tokens[0];
}

void PacketManager::CompleteCommand(const std::string &query_type, int rows,
                                    ResponseBuffer &responses) {
  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'C';
  std::string tag = query_type;
  /* After Begin, we enter a txn block */
  if (query_type.compare("BEGIN") == 0) txn_state = TXN_BLOCK;
  /* After commit, we end the txn block */
  else if (query_type.compare("COMMIT") == 0)
    txn_state = TXN_IDLE;
  /* After rollback, the txn block is ended */
  else if (!query_type.compare("ROLLBACK"))
    txn_state = TXN_IDLE;
  /* the rest are custom status messages for each command */
  else if (!query_type.compare("INSERT"))
    tag += " 0 " + std::to_string(rows);
  else
    tag += " " + std::to_string(rows);
  LOG_TRACE("complete command tag: %s", tag.c_str());
  PacketPutString(pkt, tag);

  responses.push_back(std::move(pkt));
}

/*
 * put_empty_query_response - Informs the client that an empty query was sent
 */
void PacketManager::SendEmptyQueryResponse(ResponseBuffer &responses) {
  std::unique_ptr<Packet> response(new Packet());
  response->msg_type = 'I';
  responses.push_back(std::move(response));
}

bool PacketManager::HardcodedExecuteFilter(std::string query_type) {
  // Skip SET
  if (query_type.compare("SET") == 0 || query_type.compare("SHOW") == 0)
    return false;
  // skip duplicate BEGIN
  if (!query_type.compare("BEGIN") && txn_state == TXN_BLOCK) return false;
  // skip duplicate Commits
  if (!query_type.compare("COMMIT") && txn_state == TXN_IDLE) return false;
  // skip duplicate Rollbacks
  if (!query_type.compare("ROLLBACK") && txn_state == TXN_IDLE) return false;
  return true;
}

// The Simple Query Protocol
void PacketManager::ExecQueryMessage(Packet *pkt, ResponseBuffer &responses) {
  std::string q_str;
  PacketGetString(pkt, pkt->len, q_str);

  LOG_INFO("Query Received: %s \n", q_str.c_str());
  std::vector<std::string> queries;
  boost::split(queries, q_str, boost::is_any_of(";"));

  // just a ';' sent
  if (queries.size() == 1) {
    SendEmptyQueryResponse(responses);
    SendReadyForQuery(txn_state, responses);
    return;
  }

  // Get traffic cop
  auto &tcop = tcop::TrafficCop::GetInstance();

  // iterate till before the trivial string after the last ';'
  for (auto query : queries) {
    if (query.empty()) {
      SendEmptyQueryResponse(responses);
      SendReadyForQuery(TXN_IDLE, responses);
      return;
    }

    std::vector<ResultType> result;
    std::vector<FieldInfoType> tuple_descriptor;
    std::string error_message;
    int rows_affected;

    // execute the query in Sqlite
    auto status = tcop.ExecuteStatement(query, result, tuple_descriptor,
                                        rows_affected, error_message);

    // check status
    if (status == Result::RESULT_FAILURE) {
      SendErrorResponse({{'M', error_message}}, responses);
      LOG_TRACE("Error Response Sent!");
      break;
    }

    // send the attribute names
    PutTupleDescriptor(tuple_descriptor, responses);

    // send the result rows
    SendDataRows(result, tuple_descriptor.size(), rows_affected, responses);

    // TODO: should change to query_type
    CompleteCommand(query, rows_affected, responses);
  }
  SendReadyForQuery('I', responses);
}

/*
 * exec_parse_message - handle PARSE message
 */
void PacketManager::ExecParseMessage(Packet *pkt, ResponseBuffer &responses) {
  LOG_DEBUG("Parse message");
  std::string error_message, statement_name, query_string, query_type;
  GetStringToken(pkt, statement_name);

  // Read prepare statement name
  LOG_DEBUG("Prep stmt: %s", statement_name.c_str());
  // Read query string
  GetStringToken(pkt, query_string);
  LOG_DEBUG("Parse Query: %s", query_string.c_str());
  skipped_stmt_ = false;
  query_type = get_query_type(query_string);
  if (!HardcodedExecuteFilter(query_type)) {
    // query to be filtered, don't execute
    skipped_stmt_ = true;
    skipped_query_string_ = std::move(query_string);
    skipped_query_type_ = std::move(query_type);
    LOG_TRACE("Statement to be skipped");

    // Send Parse complete response
    std::unique_ptr<Packet> response(new Packet());
    response->msg_type = '1';
    responses.push_back(std::move(response));
    return;
  }
  // Prepare statement
  std::shared_ptr<Statement> statement;
  auto &tcop = tcop::TrafficCop::GetInstance();
  statement =
      tcop.PrepareStatement(statement_name, query_string, error_message);
  if (statement.get() == nullptr) {
    SendErrorResponse({{'M', error_message}}, responses);
    SendReadyForQuery(txn_state, responses);
    return;
  }

  // Read number of params
  int num_params = PacketGetInt(pkt, 2);
  LOG_TRACE("NumParams: %d", num_params);

  // Read param types
  std::vector<int32_t> param_types(num_params);
  for (int i = 0; i < num_params; i++) {
    int param_type = PacketGetInt(pkt, 4);
    param_types[i] = param_type;
  }

  // Cache the received query
  bool unnamed_query = statement_name.empty();
  statement->SetQueryType(query_type);
  statement->SetParamTypes(param_types);

  // Unnamed statement
  if (unnamed_query) {
    unnamed_statement = statement;
  } else {
    LOG_TRACE("Setting named statement with name : %s", statement_name.c_str());
    auto entry = std::make_pair(statement_name, statement);
    statement_cache_.insert(entry);
    LOG_TRACE("CACHE SIZE: %d", (int)statement_cache_.size());
  }
  // Send Parse complete response
  std::unique_ptr<Packet> response(new Packet());
  response->msg_type = '1';
  responses.push_back(std::move(response));
}

void PacketManager::ExecBindMessage(Packet *pkt, ResponseBuffer &responses) {
  std::string portal_name, statement_name;
  // BIND message
  LOG_DEBUG("Bind Message");
  GetStringToken(pkt, portal_name);
  LOG_TRACE("Portal name: %s", portal_name.c_str());
  GetStringToken(pkt, statement_name);
  LOG_TRACE("Prep stmt name: %s", statement_name.c_str());

  if (skipped_stmt_) {
    // send bind complete
    std::unique_ptr<Packet> response(new Packet());
    response->msg_type = '2';
    responses.push_back(std::move(response));
    return;
  }

  // Read parameter format
  int num_params_format = PacketGetInt(pkt, 2);

  // get the format of each parameter
  std::vector<int16_t> formats(num_params_format);
  for (int i = 0; i < num_params_format; i++) {
    formats[i] = PacketGetInt(pkt, 2);
  }

  // error handling
  int num_params = PacketGetInt(pkt, 2);
  if (num_params_format != num_params) {
    std::string error_message =
        "Malformed request: num_params_format is not equal to num_params";
    SendErrorResponse({{'M', error_message}}, responses);
    return;
  }

  // Get statement info generated in PARSE message
  std::shared_ptr<Statement> statement;

  if (statement_name.empty()) {
    statement = unnamed_statement;

    // Check unnamed statement
    if (statement.get() == nullptr) {
      std::string error_message = "Invalid unnamed statement";
      LOG_ERROR("%s", error_message.c_str());
      SendErrorResponse({{'M', error_message}}, responses);
      return;
    }
  } else {
    auto statement_cache_itr = statement_cache_.find(statement_name);
    // Did not find statement with same name
    if (statement_cache_itr != statement_cache_.end()) {
      statement = *statement_cache_itr;
    }
    // Found statement with same name
    else {
      std::string error_message = "Prepared statement name already exists";
      LOG_ERROR("%s", error_message.c_str());
      SendErrorResponse({{'M', error_message}}, responses);
      return;
    }
  }

  const auto &query_string = statement->GetQueryString();
  const auto &query_type = statement->GetQueryType();

  // check if the loaded statement needs to be skipped
  skipped_stmt_ = false;
  if (!HardcodedExecuteFilter(query_type)) {
    skipped_stmt_ = true;
    skipped_query_string_ = query_string;
    LOG_TRACE("Statement skipped: %s", skipped_query_string_.c_str());
    std::unique_ptr<Packet> response(new Packet());
    // Send Parse complete response
    response->msg_type = '2';
    responses.push_back(std::move(response));
    return;
  }

  // Group the parameter types and the parameters in this vector
  std::vector<std::pair<int, std::string>> bind_parameters;
  auto param_types = statement->GetParamTypes();

  auto param_values = new std::vector<Value>();

  PktBuf param;
  for (int param_idx = 0; param_idx < num_params; param_idx++) {
    int param_len = PacketGetInt(pkt, 4);
    // BIND packet NULL parameter case
    if (param_len == -1) {
      // NULL mode
      bind_parameters.push_back(
          std::make_pair(ValueType::VALUE_TYPE_INTEGER, std::string("")));
    } else {
      PacketGetBytes(pkt, param_len, param);

      if (formats[param_idx] == 0) {
        // TEXT mode
        LOG_TRACE("param %d type: %d", param_idx, param_types[param_idx]);
        std::string param_str = std::string(std::begin(param), std::end(param));
        bind_parameters.push_back(
            std::make_pair(ValueType::VALUE_TYPE_VARCHAR, param_str));
        param_values->push_back(
            (ValueFactory::GetStringValue(param_str))
                .CastAs(PostgresValueTypeToPelotonValueType(
                     (PostgresValueType)param_types[param_idx])));
      } else {
        LOG_TRACE("param %d type: %d", param_idx, param_types[param_idx]);
        // BINARY mode
        switch (param_types[param_idx]) {
          case POSTGRES_VALUE_TYPE_INTEGER: {
            int int_val = 0;
            for (size_t i = 0; i < sizeof(int); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters.push_back(std::make_pair(
                ValueType::VALUE_TYPE_INTEGER, std::to_string(int_val)));
            param_values->push_back(ValueFactory::GetIntegerValue(int_val));
          } break;
          case POSTGRES_VALUE_TYPE_DOUBLE: {
            double float_val = 0;
            unsigned long buf = 0;
            for (size_t i = 0; i < sizeof(double); ++i) {
              buf = (buf << 8) | param[i];
            }
            memcpy(&float_val, &buf, sizeof(double));
            bind_parameters.push_back(std::make_pair(
                ValueType::VALUE_TYPE_DOUBLE, std::to_string(float_val)));
            param_values->push_back(ValueFactory::GetDoubleValue(float_val));
            // LOG_TRACE("Bind param (size: %d) : %lf", param_len, float_val);
          } break;
          default: {
            LOG_ERROR("Do not support data type: %d", param_types[param_idx]);
          } break;
        }
      }
    }
  }
  // Construct a portal

  LOG_TRACE("Size of param values vector: %lu", param_values->size());

  if (param_values->size() > 0) {
    LOG_TRACE("Setting Parameter Values...");
    statement->GetPlanTree()->SetParameterValues(param_values);
  }

  auto portal = new Portal(portal_name, statement, bind_parameters);
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
  std::unique_ptr<Packet> response(new Packet());
  response->msg_type = '2';
  responses.push_back(std::move(response));
}

void PacketManager::ExecDescribeMessage(Packet *pkt,
                                        ResponseBuffer &responses) {
  PktBuf mode;
  std::string portal_name;
  LOG_INFO("Describe message");
  PacketGetBytes(pkt, 1, mode);
  LOG_TRACE("mode %c", mode[0]);
  GetStringToken(pkt, portal_name);
  LOG_TRACE("portal name: %s", portal_name.c_str());
  if (mode[0] == 'P') {

    auto portal_itr = portals_.find(portal_name);

    // TODO: error handling here
    if (portal_itr == portals_.end()) {
      LOG_ERROR("Did not find portal : %s", portal_name.c_str());
      std::vector<FieldInfoType> tuple_descriptor;
      PutTupleDescriptor(tuple_descriptor, responses);
      return;
    }

    auto portal = portal_itr->second;
    if (portal == nullptr) {
      LOG_ERROR("Portal does not exist : %s", portal_name.c_str());
      std::vector<FieldInfoType> tuple_descriptor;
      PutTupleDescriptor(tuple_descriptor, responses);
      return;
    }

    auto statement = portal->GetStatement();
    PutTupleDescriptor(statement->GetTupleDescriptor(), responses);
  }
}

void PacketManager::ExecExecuteMessage(Packet *pkt, ResponseBuffer &responses) {
  // EXECUTE message
  LOG_DEBUG("Execute message");
  std::vector<ResultType> results;
  std::string error_message, portal_name;
  int rows_affected = 0;
  GetStringToken(pkt, portal_name);

  // covers weird JDBC edge case of sending double BEGIN statements. Don't
  // execute them
  if (skipped_stmt_) {
    LOG_TRACE("Statement skipped: %s", skipped_query_string_.c_str());
    CompleteCommand(skipped_query_type_, rows_affected, responses);
    skipped_stmt_ = false;
    return;
  }

  auto portal = portals_[portal_name];
  if (portal.get() == nullptr) {
    LOG_ERROR("Did not find portal : %s", portal_name.c_str());
    SendErrorResponse({{'M', error_message}}, responses);
    SendReadyForQuery(txn_state, responses);
    return;
  }

  auto statement = portal->GetStatement();
  const auto &query_type = statement->GetQueryType();
  if (statement.get() == nullptr) {
    LOG_ERROR("Did not find statement in portal : %s", portal_name.c_str());
    SendErrorResponse({{'M', error_message}}, responses);
    SendReadyForQuery(txn_state, responses);
    return;
  }

  auto statement_name = statement->GetStatementName();
  bool unnamed = statement_name.empty();

  auto &tcop = tcop::TrafficCop::GetInstance();
  auto status = tcop.ExecuteStatement(statement, unnamed, results,
                                      rows_affected, error_message);

  if (status == Result::RESULT_FAILURE) {
    LOG_ERROR("Failed to execute: %s", error_message.c_str());
    SendErrorResponse({{'M', error_message}}, responses);
    SendReadyForQuery(txn_state, responses);
  }
  // put_row_desc(portal->rowdesc, responses);
  auto tuple_descriptor = statement->GetTupleDescriptor();
  SendDataRows(results, tuple_descriptor.size(), rows_affected, responses);
  CompleteCommand(query_type, rows_affected, responses);
}

/*
 * process_packet - Main switch block; process incoming packets,
 *  Returns false if the session needs to be closed.
 */
bool PacketManager::ProcessPacket(Packet *pkt, ResponseBuffer &responses) {
  switch (pkt->msg_type) {
    case 'Q': {
      ExecQueryMessage(pkt, responses);
    } break;
    case 'P': {
      ExecParseMessage(pkt, responses);
    } break;
    case 'B': {
      ExecBindMessage(pkt, responses);
    } break;
    case 'D': {
      ExecDescribeMessage(pkt, responses);
    } break;
    case 'E': {
      ExecExecuteMessage(pkt, responses);
    } break;
    case 'S': {
      // SYNC message
      SendReadyForQuery(txn_state, responses);
    } break;
    case 'X': {
      LOG_TRACE("Closing client");
      return false;
    } break;
    default: {
      LOG_ERROR("Packet type not supported yet: %d (%c)", pkt->msg_type,
                pkt->msg_type);
    }
  }
  return true;
}

/*
 * send_error_response - Sends the passed string as an error response.
 *    For now, it only supports the human readable 'M' message body
 */
void PacketManager::SendErrorResponse(
    std::vector<std::pair<uchar, std::string>> error_status,
    ResponseBuffer &responses) {
  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'E';

  for (auto entry : error_status) {
    PacketPutByte(pkt, entry.first);
    PacketPutString(pkt, entry.second);
  }

  // put null terminator
  PacketPutByte(pkt, 0);

  // don't care if write finished or not, we are closing anyway
  responses.push_back(std::move(pkt));
}

void PacketManager::SendReadyForQuery(uchar txn_status,
                                      ResponseBuffer &responses) {
  LOG_DEBUG("Send Read for Query");
  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'Z';

  PacketPutByte(pkt, txn_status);

  responses.push_back(std::move(pkt));
}

/*
 * PacketManager - Main wire protocol logic.
 *    Always return with a closed socket.
 */
void PacketManager::ManagePackets() {
  Packet pkt;
  ResponseBuffer responses;
  bool status;

  // fetch the startup packet
  if (!ReadPacket(&pkt, false, &client)) {
    CloseClient();
    return;
  }

  status = ProcessStartupPacket(&pkt, responses);
  if (!WritePackets(responses, &client) || !status) {
    // close client on write failure or status failure
    CloseClient();
    return;
  }

  pkt.Reset();
  while (ReadPacket(&pkt, true, &client)) {
    status = ProcessPacket(&pkt, responses);
    if (!WritePackets(responses, &client) || !status) {
      // close client on write failure or status failure
      CloseClient();
      return;
    }
    pkt.Reset();
  }
}

}  // End wire namespace
}  // End peloton namespace
