//
// Created by Siddharth Santurkar on 31/3/16.
//

#include "marshall.h"
#include "cache.h"
#include "portal.h"
#include "cache_entry.h"
#include "types.h"
#include <stdio.h>
#include <boost/algorithm/string.hpp>
#include <unordered_map>

#define PROTO_MAJOR_VERSION(x) x >> 16

namespace peloton {
namespace wire {

// Prepares statment cache
thread_local Cache<std::string, CacheEntry> cache_;

// Query portal handler
thread_local std::unordered_map<std::string, std::shared_ptr<Portal>> portals_;

// Hardcoded authentication strings used during session startup. To be removed
const std::unordered_map<std::string, std::string> PacketManager::parameter_status_map =
  boost::assign::map_list_of ("application_name","psql")("client_encoding", "UTF8")
  ("DateStyle", "ISO, MDY")("integer_datetimes", "on")
  ("IntervalStyle", "postgres")("is_superuser", "on")
  ("server_encoding", "UTF8")("server_version", "9.5devel")
  ("session_authorization", "postgres")("standard_conforming_strings", "on")
  ("TimeZone", "US/Eastern");

/*
 * close_client - Close the socket of the underlying client
 */
void PacketManager::close_client() { client.sock->close_socket(); }


void PacketManager::make_hardcoded_parameter_status(
    ResponseBuffer &responses,
    const std::pair<std::string, std::string>& kv) {
  std::unique_ptr<Packet> response(new Packet());
  response->msg_type = 'S';
  packet_putstring(response, kv.first);
  packet_putstring(response, kv.second);
  responses.push_back(std::move(response));
}
/*
 * process_startup_packet - Processes the startup packet
 * 	(after the size field of the header).
 */
bool PacketManager::process_startup_packet(Packet* pkt,
                                           ResponseBuffer& responses) {
  std::string token, value;
  std::unique_ptr<Packet> response(new Packet());

  int32_t proto_version = packet_getint(pkt, sizeof(int32_t));

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
    get_string_token(pkt, token);

    // if the option database was found
    if (token.compare("database") == 0) {
      // loop end?
      if (pkt->ptr >= pkt->len) break;
      get_string_token(pkt, client.dbname);
    } else if (token.compare(("user")) == 0) {
      // loop end?
      if (pkt->ptr >= pkt->len) break;
      get_string_token(pkt, client.user);
    } else {
      if (pkt->ptr >= pkt->len) break;
      get_string_token(pkt, value);
      client.cmdline_options[token] = value;
    }
  }

  // send auth-ok ('R')
  response->msg_type = 'R';
  packet_putint(response, 0, 4);
  responses.push_back(std::move(response));

  // Send the parameterStatus map ('S')
  for(auto it = parameter_status_map.begin(); it != parameter_status_map.end(); it++) {
    make_hardcoded_parameter_status(responses, *it);
  }

  // ready-for-query packet -> 'Z'
  send_ready_for_query(TXN_IDLE, responses);
  return true;
}

void PacketManager::put_row_desc(std::vector<wiredb::FieldInfoType> &rowdesc, ResponseBuffer &responses) {
  if (!rowdesc.size())
    return;

  LOG_INFO("Put RowDescription");
  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'T';
  packet_putint(pkt, rowdesc.size(), 2);

  for (auto col : rowdesc) {
    LOG_INFO("column name: %s", std::get<0>(col).c_str());
    packet_putstring(pkt, std::get<0>(col));
    // TODO: Table Oid (int32)
    packet_putint(pkt, 0, 4);
    // TODO: Attr id of column (int16)
    packet_putint(pkt, 0, 2);
    // Field data type (int32)
    packet_putint(pkt, std::get<1>(col), 4);
    // Data type size (int16)
    packet_putint(pkt, std::get<2>(col), 2);
    // Type modifier (int32)
    packet_putint(pkt, -1, 4);
    // Format code for text
    packet_putint(pkt, 0, 2);
  }
  responses.push_back(std::move(pkt));
}


void PacketManager::send_data_rows(std::vector<wiredb::ResType> &results,
                                   int colcount,
                                   int &rows_affected,
                                   ResponseBuffer &responses) {
  if (!results.size() || !colcount)
    return;

  LOG_INFO("Flatten result size: %lu", results.size());
  size_t numrows = results.size() / colcount;

  // 1 packet per row
  for (size_t i = 0; i < numrows; i++) {
    std::unique_ptr<Packet> pkt(new Packet());
    pkt->msg_type = 'D';
    packet_putint(pkt, colcount, 2);
    for (int j = 0; j < colcount; j++) {
      // length of the row attribute
      packet_putint(pkt, results[i*colcount + j].second.size(), 4);
      // contents of the row attribute
      packet_putbytes(pkt, results[i*colcount + j].second);
    }
    responses.push_back(std::move(pkt));
  }
  rows_affected = numrows;
  LOG_INFO("Rows affected: %d", rows_affected);
}

/* Gets the first token of a query */
std::string get_query_type(std::string query) {
  std::vector<std::string> query_tokens;
  boost::split(query_tokens, query, boost::is_any_of(" "), boost::token_compress_on);
  return query_tokens[0];
}

void PacketManager::complete_command(const std::string &query_type,
                                     int rows, ResponseBuffer& responses) {
  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'C';
  std::string tag = query_type;
  /* After Begin, we enter a txn block */
  if(query_type.compare("BEGIN") == 0)
    txn_state = TXN_BLOCK;
  /* After commit, we end the txn block */
  else if(query_type.compare("COMMIT") == 0)
    txn_state = TXN_IDLE;
  /* After rollback, the txn block is ended */
  else if(!query_type.compare("ROLLBACK"))
    txn_state = TXN_IDLE;
  /* the rest are custom status messages for each command */
  else if(!query_type.compare("INSERT"))
    tag += " 0 " + std::to_string(rows);
  else
    tag += " " + std::to_string(rows);
  LOG_INFO("complete command tag: %s", tag.c_str());
  packet_putstring(pkt, tag);

  responses.push_back(std::move(pkt));
}

/*
 * put_empty_query_response - Informs the client that an empty query was sent
 */
void PacketManager::send_empty_query_response(ResponseBuffer& responses) {
  std::unique_ptr<Packet> response(new Packet());
  response->msg_type = 'I';
  responses.push_back(std::move(response));
}

bool PacketManager::hardcoded_execute_filter(std::string query_type) {
  // Skip SET
  if(query_type.compare("SET") == 0 || query_type.compare("SHOW") == 0)
    return false;
  // skip duplicate BEGIN
  if (!query_type.compare("BEGIN") && txn_state == TXN_BLOCK)
    return false;
  // skip duplicate Commits
  if (!query_type.compare("COMMIT") && txn_state == TXN_IDLE)
    return false;
  // skip duplicate Rollbacks
  if (!query_type.compare("ROLLBACK") && txn_state == TXN_IDLE)
    return false;
  return true;
}

// TODO: rewrite this method
/* The Simple Query Protocol */
void PacketManager::exec_query_message(Packet *pkt, ResponseBuffer &responses) {
  std::string q_str;
  packet_getstring(pkt, pkt->len, q_str);
  LOG_INFO("Query Received: %s \n", q_str.c_str());

  std::vector<std::string> queries;
  boost::split(queries, q_str, boost::is_any_of(";"));

  // just a ';' sent
  if (queries.size() == 1) {
    send_empty_query_response(responses);
    send_ready_for_query(txn_state, responses);
    return;
  }

  // iterate till before the trivial string after the last ';'
  for (auto query = queries.begin(); query != queries.end() - 1; query++) {
    if (query->empty()) {
      send_empty_query_response(responses);
      send_ready_for_query(TXN_IDLE, responses);
      return;
    }

    std::vector<wiredb::ResType> results;
    std::vector<wiredb::FieldInfoType> rowdesc;
    std::string err_msg;
    int rows_affected;

    // execute the query in Sqlite
    int isfailed =  db.PortalExec(query->c_str(), results, rowdesc, rows_affected, err_msg);

    if(isfailed) {
      send_error_response({{'M', err_msg}}, responses);
      break;
    }

    // send the attribute names
    put_row_desc(rowdesc, responses);

    // send the result rows
    send_data_rows(results, rowdesc.size(), rows_affected, responses);

    // TODO: should change to query_type
    complete_command(*query, rows_affected, responses);
  }

  send_ready_for_query('I', responses);
}

/*
 * exec_parse_message - handle PARSE message
 */
void PacketManager::exec_parse_message(Packet *pkt, ResponseBuffer &responses) {
  LOG_INFO("PARSE message");
  std::string err_msg, prep_stmt_name, query, query_type;
  get_string_token(pkt, prep_stmt_name);

  // Read prepare statement name
  sqlite3_stmt *stmt = nullptr;
  LOG_INFO("Prep stmt: %s", prep_stmt_name.c_str());
  // Read query string
  get_string_token(pkt, query);
  LOG_INFO("Parse Query: %s", query.c_str());

  skipped_stmt_ = false;
  query_type = get_query_type(query);
  if (!hardcoded_execute_filter(query_type)) {
    // query to be filtered, don't execute
    skipped_stmt_ = true;
    skipped_query_ = std::move(query);
    skipped_query_type_ = std::move(query_type);
    LOG_INFO("Statement to be skipped");
  } else {
    // Prepare statement
    int is_failed = db.PrepareStmt(query.c_str(), &stmt, err_msg);
    if (is_failed) {
      send_error_response({{'M', err_msg}}, responses);
      send_ready_for_query(txn_state, responses);
    }
  }

  // Read number of params
  int num_params = packet_getint(pkt, 2);
  LOG_INFO("NumParams: %d", num_params);

  // Read param types
  std::vector<int32_t> param_types(num_params);
  for (int i = 0; i < num_params; i++) {
    int param_type = packet_getint(pkt, 4);
    param_types[i] = param_type;
  }

  // Cache the received qury
  std::shared_ptr<CacheEntry> entry(new CacheEntry());
  entry->stmt_name = prep_stmt_name;
  entry->query_string = query;
  entry->query_type = std::move(query_type);
  entry->sql_stmt = stmt;
  entry->param_types = std::move(param_types);

  if (prep_stmt_name.empty()) {
    // Unnamed statement
    unnamed_entry = entry;
  } else {
    cache_.insert(std::make_pair(std::move(prep_stmt_name), entry));
  }

  std::unique_ptr<Packet> response(new Packet());

  // Send Parse complete response
  response->msg_type = '1';
  responses.push_back(std::move(response));
}

void PacketManager::exec_bind_message(Packet *pkt, ResponseBuffer &responses) {
  std::string portal_name, prep_stmt_name;
  // BIND message
  LOG_INFO("BIND message");
  get_string_token(pkt, portal_name);
  LOG_INFO("Portal name: %s", portal_name.c_str());
  get_string_token(pkt, prep_stmt_name);
  LOG_INFO("Prep stmt name: %s", prep_stmt_name.c_str());

  if (skipped_stmt_) {
    //send bind complete
    std::unique_ptr<Packet> response(new Packet());
    response->msg_type = '2';
    responses.push_back(std::move(response));
    return;
  }

  // Read parameter format
  int num_params_format = packet_getint(pkt, 2);

  // get the format of each parameter
  std::vector<int16_t> formats(num_params_format);
  for (int i = 0; i < num_params_format; i++) {
    formats[i] = packet_getint(pkt, 2);
  }

  // error handling
  int num_params = packet_getint(pkt, 2);
  if (num_params_format != num_params) {
    std::string err_msg =
      "Malformed request: num_params_format is not equal to num_params";
    send_error_response({{'M', err_msg}}, responses);
    return;
  }

  // Get statement info generated in PARSE message
  sqlite3_stmt *stmt = nullptr;
  std::shared_ptr<CacheEntry> entry;
  if (prep_stmt_name.empty()) {
    LOG_INFO("Unnamed statement");
    entry = unnamed_entry;
  } else {
    // fetch the statement ID from the cache
    auto itr = cache_.find(prep_stmt_name);
    if (itr != cache_.end()) {
      entry = *itr;
    } else {
      std::string err_msg = "Prepared statement name already exists";
      send_error_response({{'M', err_msg}}, responses);
      return;
    }
  }
  stmt = entry->sql_stmt;
  const auto &query_string = entry->query_string;
  const auto &query_type = entry->query_type;

  // check if the loaded statement needs to be skipped
  skipped_stmt_ = false;
  if (!hardcoded_execute_filter(query_type)) {
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
    int param_len = packet_getint(pkt, 4);
    // BIND packet NULL parameter case
    if (param_len == -1) {
      // NULL mode
      bind_parameters.push_back(std::make_pair(WIRE_NULL, std::string("")));
    } else {
      packet_getbytes(pkt, param_len, param);

      if (formats[param_idx] == 0) {
        // TEXT mode
        std::string param_str = std::string(std::begin(param),
            std::end(param));
        bind_parameters.push_back(std::make_pair(WIRE_TEXT, param_str));
      } else {
        // BINARY mode
        switch (entry->param_types[param_idx]) {
          case POSTGRES_VALUE_TYPE_INTEGER: {
            int int_val = 0;
            for (size_t i = 0; i < sizeof(int); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters.push_back(
                std::make_pair(WIRE_INTEGER, std::to_string(int_val)));
          }
            break;
          case POSTGRES_VALUE_TYPE_DOUBLE: {
            double float_val = 0;
            unsigned long buf = 0;
            for (size_t i = 0; i < sizeof(double); ++i) {
              buf = (buf << 8) | param[i];
            }
            memcpy(&float_val, &buf, sizeof(double));
            bind_parameters.push_back(
                std::make_pair(WIRE_FLOAT, std::to_string(float_val)));
            // LOG_INFO("Bind param (size: %d) : %lf", param_len, float_val);
          }
            break;
          default: {
            LOG_ERROR("Do not support data type: %d",
                      entry->param_types[param_idx]);
          }
            break;
        }
      }
    }
  }

  std::string err_msg;
  bool is_failed = db.BindStmt(bind_parameters, &stmt, err_msg);
  if (is_failed) {
    send_error_response({{'M', err_msg}}, responses);
    send_ready_for_query(txn_state, responses);
  }

  // TODO: replace this with a constructor
  std::shared_ptr<Portal> portal(new Portal());
  portal->query_string = query_string;
  portal->stmt = stmt;
  portal->prep_stmt_name = prep_stmt_name;
  portal->portal_name = portal_name;
  portal->query_type = query_type;
  portal->colcount = 0;

  auto itr = portals_.find(portal_name);
  if (itr == portals_.end()) {
    portals_.insert(std::make_pair(portal_name, portal));
  } else {
    std::shared_ptr<Portal> p = itr->second;
    itr->second = portal;
  }

  //send bind complete
  std::unique_ptr<Packet> response(new Packet());
  response->msg_type = '2';
  responses.push_back(std::move(response));
}

void PacketManager::exec_describe_message(Packet *pkt,
                                          ResponseBuffer &responses) {
  PktBuf mode;
  std::string name;
  LOG_INFO("DESCRIBE message");
  packet_getbytes(pkt, 1, mode);
  LOG_INFO("mode %c", mode[0]);
  get_string_token(pkt, name);
  LOG_INFO("name: %s", name.c_str());
  if (mode[0] == 'P') {
    auto portal_itr = portals_.find(name);
    if (portal_itr == portals_.end()) {
      // TODO: error handling here
      std::vector<wiredb::FieldInfoType> rowdesc;
      put_row_desc(rowdesc, responses);
      return;
    }
    std::shared_ptr<Portal> p = portal_itr->second;
    db.GetRowDesc(p->stmt, p->rowdesc);
    put_row_desc(p->rowdesc, responses);
    p->colcount = p->rowdesc.size();
  } else if (mode[0] == 'S') {
    // TODO: need to handle this case
  } else {
    // TODO: error handling here
  }
}

void PacketManager::exec_execute_message(Packet *pkt,
                                         ResponseBuffer &responses, ThreadGlobals& globals) {
  // EXECUTE message
  LOG_INFO("EXECUTE message");
  std::vector<wiredb::ResType> results;
  std::string err_msg, portal_name;
  sqlite3_stmt *stmt = nullptr;
  int rows_affected = 0, is_failed;
  get_string_token(pkt, portal_name);

  // covers weird JDBC edge case of sending double BEGIN statements. Don't execute them
  if (skipped_stmt_) {
    LOG_INFO("Statement skipped: %s", skipped_query_.c_str());
    complete_command(skipped_query_type_, rows_affected, responses);
    skipped_stmt_ = false;
    return;
  }

  auto portal = portals_[portal_name];
  const auto &query_string = portal->query_string;
  const auto &query_type = portal->query_type;
  stmt = portal->stmt;
  ASSERT(stmt != nullptr);

  bool unnamed = portal->prep_stmt_name.empty();

  LOG_INFO("Executing query: %s", query_string.c_str());

  // acquire the mutex if we are starting a txn
  if (query_string.compare("BEGIN") == 0) {
    LOG_WARN("BEGIN - acquire lock");
    globals.sqlite_mutex.lock();
  }

  is_failed = db.ExecPrepStmt(stmt, unnamed, results, rows_affected, err_msg);
  if (is_failed) {
    LOG_INFO("Failed to execute: %s", err_msg.c_str());
    send_error_response({{'M', err_msg}}, responses);
    send_ready_for_query(txn_state, responses);
  }

  // release the mutex after a txn commit
  if (query_string.compare("COMMIT") == 0) {
    LOG_WARN("COMMIT - release lock");
    globals.sqlite_mutex.unlock();
  }

  if (portal->colcount == 0){
    // colcount uninitialized, load the colcount
    db.GetRowDesc(portal->stmt, portal->rowdesc);
    portal->colcount = portal->rowdesc.size();
  }

  //put_row_desc(portal->rowdesc, responses);
  send_data_rows(results, portal->colcount, rows_affected, responses);
  complete_command(query_type, rows_affected, responses);
}

/*
 * process_packet - Main switch block; process incoming packets,
 *  Returns false if the seesion needs to be closed.
 */
bool PacketManager::process_packet(Packet* pkt, ThreadGlobals& globals, ResponseBuffer& responses) {
  switch (pkt->msg_type) {
    case 'Q': {
      exec_query_message(pkt, responses);
    } break;
    case 'P': {
      exec_parse_message(pkt, responses);
    } break;
    case 'B': {
      exec_bind_message(pkt, responses);
    } break;
    case 'D': {
      exec_describe_message(pkt, responses);
    } break;
    case 'E': {
      exec_execute_message(pkt, responses, globals);
    } break;
    case 'S': {
      // SYNC message
      send_ready_for_query(txn_state, responses);
    } break;
    case 'X': {
      LOG_INFO("Closing client");
      return false;
    } break;
    default: {
      LOG_INFO("Packet type not supported yet: %d (%c)", pkt->msg_type, pkt->msg_type);
    }
  }
  return true;
}

/*
 * send_error_response - Sends the passed string as an error response.
 * 		For now, it only supports the human readable 'M' message body
 */
void PacketManager::send_error_response(
    std::vector<std::pair<uchar, std::string>> error_status,
    ResponseBuffer& responses) {
  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'E';

  for (auto entry : error_status) {
    packet_putbyte(pkt, entry.first);
    packet_putstring(pkt, entry.second);
  }

  // put null terminator
  packet_putbyte(pkt, 0);

  // don't care if write finished or not, we are closing anyway
  responses.push_back(std::move(pkt));
}

void PacketManager::send_ready_for_query(uchar txn_status,
                                         ResponseBuffer& responses) {
  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'Z';

  packet_putbyte(pkt, txn_status);

  responses.push_back(std::move(pkt));
}

/*
 * PacketManager - Main wire protocol logic.
 * 		Always return with a closed socket.
 */
void PacketManager::manage_packets(ThreadGlobals& globals) {
  Packet pkt;
  ResponseBuffer responses;
  bool status;

  // fetch the startup packet
  if (!read_packet(&pkt, false, &client)) {
    close_client();
    return;
  }

  status = process_startup_packet(&pkt, responses);
  if (!write_packets(responses, &client) || !status) {
    // close client on write failure or status failure
    close_client();
    return;
  }

  pkt.reset();
  while (read_packet(&pkt, true, &client)) {
    status = process_packet(&pkt, globals, responses);
    if (!write_packets(responses, &client) || !status) {
      // close client on write failure or status failure
      close_client();
      return;
    }
    pkt.reset();
  }
}
}
}
