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

#include <boost/algorithm/string.hpp>

#define PROTO_MAJOR_VERSION(x) x >> 16

namespace peloton {
namespace wire {

// Prepares statment cache
thread_local peloton::Cache<std::string, PreparedStatement> cache_;

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
 * 	(after the size field of the header).
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

void PacketManager::PutTupleDescriptor(const std::vector<FieldInfoType> &tuple_descriptor,
                                       ResponseBuffer &responses) {

  if (tuple_descriptor.empty())
    return;

  LOG_INFO("Put TupleDescriptor");

  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'T';
  PacketPutInt(pkt, tuple_descriptor.size(), 2);

  for (auto col : tuple_descriptor) {
    LOG_INFO("column name: %s", std::get<0>(col).c_str());
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

void PacketManager::SendDataRows(std::vector<ResultType> &results,
                                 int colcount, int &rows_affected,
                                 ResponseBuffer &responses) {
  if (!results.size() || !colcount) return;

  LOG_INFO("Flatten result size: %lu", results.size());
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
  LOG_INFO("Rows affected: %d", rows_affected);
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
  LOG_INFO("complete command tag: %s", tag.c_str());
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
    auto status = tcop.ExecuteStatement(query,
                                        result,
                                        tuple_descriptor,
                                        rows_affected,
                                        error_message);

    // check status
    if (status ==  Result::RESULT_FAILURE) {
      SendErrorResponse({{'M', error_message}}, responses);
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
  LOG_INFO("PARSE message");
  std::string error_message, prepared_statement_name, query, query_type;
  GetStringToken(pkt, prepared_statement_name);

  // Read prepare statement name
  LOG_INFO("Prep stmt: %s", prepared_statement_name.c_str());
  // Read query string
  GetStringToken(pkt, query);
  LOG_INFO("Parse Query: %s", query.c_str());

  std::shared_ptr<PreparedStatement> prepared_statement;

  skipped_stmt_ = false;
  query_type = get_query_type(query);
  if (!HardcodedExecuteFilter(query_type)) {
    // query to be filtered, don't execute
    skipped_stmt_ = true;
    skipped_query_ = std::move(query);
    skipped_query_type_ = std::move(query_type);
    LOG_INFO("Statement to be skipped");
  } else {
    // Prepare statement
    auto &tcop = tcop::TrafficCop::GetInstance();
    prepared_statement = std::move(tcop.PrepareStatement(query,
                                                         error_message));

    if (prepared_statement.get() == nullptr) {
      SendErrorResponse({{'M', error_message}}, responses);
      SendReadyForQuery(txn_state, responses);
    }

  }

  // Read number of params
  int num_params = PacketGetInt(pkt, 2);
  LOG_INFO("NumParams: %d", num_params);

  // Read param types
  std::vector<int32_t> param_types(num_params);
  for (int i = 0; i < num_params; i++) {
    int param_type = PacketGetInt(pkt, 4);
    param_types[i] = param_type;
  }

  // Cache the received qury
  std::shared_ptr<PreparedStatement> entry(new PreparedStatement());
  entry->prepared_statement_name = prepared_statement_name;
  entry->query_string = query;
  entry->query_type = std::move(query_type);
  entry->param_types = std::move(param_types);

  if (prepared_statement_name.empty()) {
    // Unnamed statement
    unnamed_entry = entry;
  } else {
    cache_.insert(std::make_pair(std::move(prepared_statement_name), entry));
  }

  std::unique_ptr<Packet> response(new Packet());

  // Send Parse complete response
  response->msg_type = '1';
  responses.push_back(std::move(response));
}

void PacketManager::ExecBindMessage(Packet *pkt, ResponseBuffer &responses) {
  std::string portal_name, prepared_statement_name;
  // BIND message
  LOG_INFO("BIND message");
  GetStringToken(pkt, portal_name);
  LOG_INFO("Portal name: %s", portal_name.c_str());
  GetStringToken(pkt, prepared_statement_name);
  LOG_INFO("Prep stmt name: %s", prepared_statement_name.c_str());

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
  std::shared_ptr<PreparedStatement> entry;
  if (prepared_statement_name.empty()) {
    LOG_INFO("Unnamed statement");
    entry = unnamed_entry;
  } else {
    // fetch the statement ID from the cache
    auto itr = cache_.find(prepared_statement_name);
    if (itr != cache_.end()) {
      entry = *itr;
    } else {
      std::string error_message = "Prepared statement name already exists";
      SendErrorResponse({{'M', error_message}}, responses);
      return;
    }
  }

  std::shared_ptr<PreparedStatement> prepared_statement = entry;
  const auto &query_string = entry->query_string;
  const auto &query_type = entry->query_type;

  // check if the loaded statement needs to be skipped
  skipped_stmt_ = false;
  if (!HardcodedExecuteFilter(query_type)) {
    skipped_stmt_ = true;
    skipped_query_ = query_string;
    LOG_INFO("Statement skipped: %s", skipped_query_.c_str());
    std::unique_ptr<Packet> response(new Packet());
    // Send Parse complete response
    response->msg_type = '2';
    responses.push_back(std::move(response));
    return;
  }

  // Group the parameter types and thae parameters in this vector
  std::vector<std::pair<int, std::string>> bind_parameters;
  PktBuf param;
  for (int param_idx = 0; param_idx < num_params; param_idx++) {
    int param_len = PacketGetInt(pkt, 4);
    // BIND packet NULL parameter case
    if (param_len == -1) {
      // NULL mode
      bind_parameters.push_back(std::make_pair(ValueType::VALUE_TYPE_INTEGER, std::string("")));
    } else {
      PacketGetBytes(pkt, param_len, param);

      if (formats[param_idx] == 0) {
        // TEXT mode
        std::string param_str = std::string(std::begin(param), std::end(param));
        bind_parameters.push_back(std::make_pair(ValueType::VALUE_TYPE_VARCHAR, param_str));
      } else {
        // BINARY mode
        switch (entry->param_types[param_idx]) {
          case POSTGRES_VALUE_TYPE_INTEGER: {
            int int_val = 0;
            for (size_t i = 0; i < sizeof(int); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters.push_back(
                std::make_pair(ValueType::VALUE_TYPE_INTEGER, std::to_string(int_val)));
          } break;
          case POSTGRES_VALUE_TYPE_DOUBLE: {
            double float_val = 0;
            unsigned long buf = 0;
            for (size_t i = 0; i < sizeof(double); ++i) {
              buf = (buf << 8) | param[i];
            }
            memcpy(&float_val, &buf, sizeof(double));
            bind_parameters.push_back(
                std::make_pair(ValueType::VALUE_TYPE_DOUBLE, std::to_string(float_val)));
            // LOG_INFO("Bind param (size: %d) : %lf", param_len, float_val);
          } break;
          default: {
            LOG_ERROR("Do not support data type: %d",
                      entry->param_types[param_idx]);
          } break;
        }
      }
    }
  }

  // TODO: replace this with a constructor
  std::shared_ptr<Portal> portal(new Portal());

  // Shared the statement with the portal
  portal->prepared_statement = prepared_statement;
  portal->portal_name = portal_name;
  portal->bind_parameters = bind_parameters;

  auto itr = portals_.find(portal_name);
  if (itr == portals_.end()) {
    portals_.insert(std::make_pair(portal_name, portal));
  } else {
    std::shared_ptr<Portal> p = itr->second;
    itr->second = portal;
  }

  // send bind complete
  std::unique_ptr<Packet> response(new Packet());
  response->msg_type = '2';
  responses.push_back(std::move(response));
}

void PacketManager::ExecDescribeMessage(Packet *pkt,
                                        ResponseBuffer &responses) {
  PktBuf mode;
  std::string name;
  LOG_INFO("DESCRIBE message");
  PacketGetBytes(pkt, 1, mode);
  LOG_INFO("mode %c", mode[0]);
  GetStringToken(pkt, name);
  LOG_INFO("name: %s", name.c_str());

  if (mode[0] == 'P') {
    auto portal_itr = portals_.find(name);
    if (portal_itr == portals_.end()) {
      // TODO: error handling here
      std::vector<FieldInfoType> tuple_descriptor;
      PutTupleDescriptor(tuple_descriptor, responses);
      return;
    }

    std::shared_ptr<Portal> portal = portal_itr->second;
    auto prepared_statement = portal->prepared_statement;
    PutTupleDescriptor(prepared_statement->tuple_descriptor, responses);
  }

}

void PacketManager::ExecExecuteMessage(Packet *pkt, ResponseBuffer &responses) {
  // EXECUTE message
  LOG_INFO("EXECUTE message");
  std::vector<ResultType> results;
  std::string error_message, portal_name;
  int rows_affected = 0;
  GetStringToken(pkt, portal_name);

  // covers weird JDBC edge case of sending double BEGIN statements. Don't
  // execute them
  if (skipped_stmt_) {
    LOG_INFO("Statement skipped: %s", skipped_query_.c_str());
    CompleteCommand(skipped_query_type_, rows_affected, responses);
    skipped_stmt_ = false;
    return;
  }

  auto portal = portals_[portal_name];
  auto prepared_statement = portal->prepared_statement;
  PL_ASSERT(prepared_statement.get() != nullptr);
  const auto &query_string = prepared_statement->query_string;
  const auto &query_type = prepared_statement->query_type;

  bool unnamed = prepared_statement->prepared_statement_name.empty();

  LOG_INFO("Executing query: %s", query_string.c_str());

  // acquire the mutex if we are starting a txn
  if (query_string.compare("BEGIN") == 0) {
    LOG_WARN("BEGIN - acquire lock");
  }

  auto &tcop = tcop::TrafficCop::GetInstance();
  auto status = tcop.ExecutePreparedStatement(prepared_statement,
                                              unnamed,
                                              results,
                                              rows_affected,
                                              error_message);
  if (status == Result::RESULT_FAILURE) {
    LOG_INFO("Failed to execute: %s", error_message.c_str());
    SendErrorResponse({{'M', error_message}}, responses);
    SendReadyForQuery(txn_state, responses);
  }

  // release the mutex after a txn commit
  if (query_string.compare("COMMIT") == 0) {
    LOG_WARN("COMMIT - release lock");
  }

  // put_row_desc(portal->rowdesc, responses);
  SendDataRows(results, portal->prepared_statement->tuple_descriptor.size(), rows_affected, responses);
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
      LOG_INFO("Closing client");
      return false;
    } break;
    default: {
      LOG_INFO("Packet type not supported yet: %d (%c)", pkt->msg_type,
               pkt->msg_type);
    }
  }
  return true;
}

/*
 * send_error_response - Sends the passed string as an error response.
 * 		For now, it only supports the human readable 'M' message body
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

/*
 * PacketManager - Main wire protocol logic.
 * 		Always return with a closed socket.
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
