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

  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'T';
  PacketPutInt(pkt, tuple_descriptor.size(), 2);

  for (auto col : tuple_descriptor) {
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
  if (results.empty() || colcount == 0) return;

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
}

/* Gets the first token of a query */
std::string get_query_type(std::string query) {
  std::string query_type;
  std::stringstream stream(query);
  stream >> query_type;
  return query_type;
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

  std::vector<std::string> queries;
  boost::split(queries, q_str, boost::is_any_of(";"));


  if (queries.size() == 1) {
    SendEmptyQueryResponse(responses);
    SendReadyForQuery(txn_state, responses);
    return;
  }

  // Get traffic cop
  auto &tcop = tcop::TrafficCop::GetInstance();

  for (auto query : queries) {
    // iterate till before the empty string after the last ';'
    if (query != queries.back()) {
      if (query.empty()) {
        SendEmptyQueryResponse(responses);
        SendReadyForQuery(TXN_IDLE, responses);
        return;
      }

      std::vector<ResultType> result;
      std::vector<FieldInfoType> tuple_descriptor;
      std::string error_message;
      int rows_affected;

      // execute the query using tcop
      auto status = tcop.ExecuteStatement(query, result, tuple_descriptor,
                                          rows_affected, error_message);

      // check status
      if (status == Result::RESULT_FAILURE) {
        SendErrorResponse({{'M', error_message}}, responses);
        break;
      }

      // send the attribute names
      PutTupleDescriptor(tuple_descriptor, responses);

      // send the result rows
      SendDataRows(result, tuple_descriptor.size(), rows_affected, responses);

      // TODO: should change to query_type
      CompleteCommand(query, rows_affected, responses);
    } else if (queries.size() == 1) {
      // just a ';' sent
      SendEmptyQueryResponse(responses);
    }
  }

  SendReadyForQuery('I', responses);
}

/*
 * exec_parse_message - handle PARSE message
 */
void PacketManager::ExecParseMessage(Packet *pkt, ResponseBuffer &responses) {
  std::string error_message, statement_name, query_string, query_type;
  GetStringToken(pkt, statement_name);

  // Read prepare statement name
  // Read query string
  GetStringToken(pkt, query_string);
  skipped_stmt_ = false;
  query_type = get_query_type(query_string);
  if (!HardcodedExecuteFilter(query_type)) {
    // query to be filtered, don't execute
    skipped_stmt_ = true;
    skipped_query_string_ = std::move(query_string);
    skipped_query_type_ = std::move(query_type);

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
    auto entry = std::make_pair(statement_name, statement);
    statement_cache_.insert(entry);
  }
  // Send Parse complete response
  std::unique_ptr<Packet> response(new Packet());
  response->msg_type = '1';
  responses.push_back(std::move(response));
}

void PacketManager::ExecBindMessage(Packet *pkt, ResponseBuffer &responses) {
  std::string portal_name, statement_name;
  // BIND message
  GetStringToken(pkt, portal_name);
  GetStringToken(pkt, statement_name);

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
    std::unique_ptr<Packet> response(new Packet());
    // Send Parse complete response
    response->msg_type = '2';
    responses.push_back(std::move(response));
    return;
  }

  // Group the parameter types and the parameters in this vector
  std::vector<std::pair<int, std::string>> bind_parameters;
  auto param_types = statement->GetParamTypes();

  std::vector<common::Value *> param_values;

  PktBuf param;
  for (int param_idx = 0; param_idx < num_params; param_idx++) {
    int param_len = PacketGetInt(pkt, 4);
    // BIND packet NULL parameter case
    if (param_len == -1) {
      // NULL mode
      bind_parameters.push_back(
          std::make_pair(common::Type::INTEGER, std::string("")));
    } else {
      PacketGetBytes(pkt, param_len, param);

      if (formats[param_idx] == 0) {
        // TEXT mode
        std::string param_str = std::string(std::begin(param), std::end(param));
        bind_parameters.push_back(
            std::make_pair(common::Type::VARCHAR, param_str));
        param_values.push_back(
            (common::ValueFactory::GetVarcharValue(param_str))
                .CastAs(PostgresValueTypeToPelotonValueType(
                     (PostgresValueType)param_types[param_idx])));
      } else {
        // BINARY mode
        switch (param_types[param_idx]) {
          case POSTGRES_VALUE_TYPE_INTEGER: {
            int int_val = 0;
            for (size_t i = 0; i < sizeof(int); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters.push_back(std::make_pair(
                common::Type::INTEGER, std::to_string(int_val)));
            param_values.push_back(common::ValueFactory::GetIntegerValue(int_val).Copy());
          } break;
          case POSTGRES_VALUE_TYPE_BIGINT: {
            int64_t int_val = 0;
            for (size_t i = 0; i < sizeof(int64_t); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters.push_back(std::make_pair(
                common::Type::BIGINT, std::to_string(int_val)));
            param_values.push_back(common::ValueFactory::GetBigIntValue(int_val).Copy());
          } break;
          case POSTGRES_VALUE_TYPE_DOUBLE: {
            double float_val = 0;
            unsigned long buf = 0;
            for (size_t i = 0; i < sizeof(double); ++i) {
              buf = (buf << 8) | param[i];
            }
            PL_MEMCPY(&float_val, &buf, sizeof(double));
            bind_parameters.push_back(std::make_pair(
                common::Type::DECIMAL, std::to_string(float_val)));
            param_values.push_back(common::ValueFactory::GetDoubleValue(float_val).Copy());
          } break;
          default: {
            LOG_ERROR("Do not support data type: %d", param_types[param_idx]);
          } break;
        }
      }
    }
  }
  // Construct a portal

  if (param_values.size() > 0) {
    statement->GetPlanTree()->SetParameterValues(&param_values);
  }

  //clean up param_values
  for (auto val_ptr : param_values){
    delete val_ptr;
  }
  param_values.clear();

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

bool PacketManager::ExecDescribeMessage(Packet *pkt,
                                        ResponseBuffer &responses) {
  PktBuf mode;
  std::string portal_name;
  PacketGetBytes(pkt, 1, mode);
  GetStringToken(pkt, portal_name);
  if (mode[0] == 'P') {

    auto portal_itr = portals_.find(portal_name);

    // TODO: error handling here
    // Ahmed: This is causing the continuously running thread
    // Changed the function signature to return boolean
    // when false is returned, the connection is closed
    if (portal_itr == portals_.end()) {
      LOG_ERROR("Did not find portal : %s", portal_name.c_str());
      std::vector<FieldInfoType> tuple_descriptor;
      PutTupleDescriptor(tuple_descriptor, responses);
      return false;
    }

    auto portal = portal_itr->second;
    if (portal == nullptr) {
      LOG_ERROR("Portal does not exist : %s", portal_name.c_str());
      std::vector<FieldInfoType> tuple_descriptor;
      PutTupleDescriptor(tuple_descriptor, responses);
      return false;
    }

    auto statement = portal->GetStatement();
    PutTupleDescriptor(statement->GetTupleDescriptor(), responses);
  }
  return true;
}

void PacketManager::ExecExecuteMessage(Packet *pkt, ResponseBuffer &responses) {
  // EXECUTE message
  std::vector<ResultType> results;
  std::string error_message, portal_name;
  int rows_affected = 0;
  GetStringToken(pkt, portal_name);

  // covers weird JDBC edge case of sending double BEGIN statements. Don't
  // execute them
  if (skipped_stmt_) {
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
      LOG_DEBUG("Q");
      ExecQueryMessage(pkt, responses);
    } break;
    case 'P': {
      LOG_DEBUG("P");
      ExecParseMessage(pkt, responses);
    } break;
    case 'B': {
      LOG_DEBUG("B");
      ExecBindMessage(pkt, responses);
    } break;
    case 'D': {
      LOG_DEBUG("D");
      return ExecDescribeMessage(pkt, responses);
    } break;
    case 'E': {
      LOG_DEBUG("E");
      ExecExecuteMessage(pkt, responses);
    } break;
    case 'S': {
      LOG_DEBUG("S");
      SendReadyForQuery(txn_state, responses);
    } break;
    case 'X': {
      LOG_DEBUG("X");
      return false;
    } break;
    case NULL: {
      LOG_DEBUG("NULL");
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
  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'Z';

  PacketPutByte(pkt, txn_status);

  responses.push_back(std::move(pkt));
}

bool PacketManager::ManageStartupPacket() {
  Packet pkt;
  ResponseBuffer responses;
  bool status;
  // fetch the startup packet
  if (!ReadPacket(&pkt, false, &client)) {
    CloseClient();
    return false;
  }
  status = ProcessStartupPacket(&pkt, responses);
  if (!WritePackets(responses, &client) || !status) {
    // close client on write failure or status failure
    CloseClient();
    return false;
  }
  return true;
}

bool PacketManager::ManagePacket() {
  Packet pkt;
  ResponseBuffer responses;
  bool status, read_status;

  read_status = ReadPacket(&pkt, true, &client);

  // While can process more packets from buffer
  while (read_status) {
    // Process the read packet
    status = ProcessPacket(&pkt, responses);

    // Write response
    if (!WritePackets(responses, &client) || status == false) {
      // close client on write failure or status failure
      CloseClient();
      return false;
    }

    pkt.Reset();
    read_status = ReadPacket(&pkt, true, &client);
  }
  return true;
}

}  // End wire namespace
}  // End peloton namespace
