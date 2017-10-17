//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// packet_manager.h
//
// Identification: src/include/network/postgres_protocol_handler.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <boost/assign/list_of.hpp>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/cache.h"
#include "common/portal.h"
#include "common/statement.h"
#include "logging/wal_log_manager.h"
#include "traffic_cop/traffic_cop.h"
#include "protocol_handler.h"
#include "type/types.h"

// Packet content macros
#define NULL_CONTENT_SIZE -1

namespace peloton {

namespace network {

typedef std::vector<std::unique_ptr<OutputPacket>> ResponseBuffer;

class PostgresProtocolHandler: public ProtocolHandler {
 public:
  // TODO we need to somehow make this virtual?
  PostgresProtocolHandler(tcop::TrafficCop *traffic_cop, logging::WalLogManager *log_manager);

  ~PostgresProtocolHandler();

  ProcessResult Process(Buffer &rbuf, const size_t thread_id);

  /* Main switch case wrapper to process every packet apart from the startup
   * packet. Avoid flushing the response for extended protocols. */
  ProcessResult ProcessPacket(InputPacket* pkt, const size_t thread_id);

  /* Manage the startup packet */
  //  bool ManageStartupPacket();
  void SendInitialResponse();
  void Reset();


  // Ugh... this should not be here but we have no choice...
  void ReplanPreparedStatement(Statement* statement);

  // Check existence of statement in cache by name
  // Return true if exists
  bool ExistCachedStatement(std::string statement_name) {
    auto statement_cache_itr = statement_cache_.find(statement_name);
    return statement_cache_itr != statement_cache_.end();
  }


  void GetResult();

  //===--------------------------------------------------------------------===//
  // STATIC HELPERS
  //===--------------------------------------------------------------------===//

  // Deserialize the parameter types from packet
  static size_t ReadParamType(InputPacket* pkt, int num_params,
                              std::vector<int32_t>& param_types);

  // Deserialize the parameter format from packet
  static size_t ReadParamFormat(InputPacket* pkt, int num_params_format,
                                std::vector<int16_t>& formats);

  // Deserialize the parameter value from packet
  static size_t ReadParamValue(
      InputPacket* pkt, int num_params, std::vector<int32_t>& param_types,
      std::vector<std::pair<type::TypeId, std::string>>& bind_parameters,
      std::vector<type::Value>& param_values, std::vector<int16_t>& formats);


  // Packet Reading Function
  // Extracts the contents of Postgres packet from the read socket buffer
  static bool ReadPacket(Buffer &rbuf, InputPacket &rpkt);


 private:

  // Packet Reading Function
  // Extracts the header of a Postgres packet from the read socket buffer
  static bool ReadPacketHeader(Buffer &rbuf, InputPacket &rpkt);

  //===--------------------------------------------------------------------===//
  // PROTOCOL HANDLING FUNCTIONS
  //===--------------------------------------------------------------------===//
   // Generic error protocol packet
  void SendErrorResponse(
      std::vector<std::pair<NetworkMessageType, std::string>> error_status);

  // Sends ready for query packet to the frontend
  void SendReadyForQuery(NetworkTransactionStateType txn_status);

  // Sends the attribute headers required by SELECT queries
  void PutTupleDescriptor(const std::vector<FieldInfo>& tuple_descriptor);

  // Send each row, one packet at a time, used by SELECT queries
  void SendDataRows(std::vector<StatementResult>& results, int colcount,
                    int& rows_affected);

  // Used to send a packet that indicates the completion of a query. Also has
  // txn state mgmt
  void CompleteCommand(const std::string& query_type_string, const QueryType& query_type, int rows);

  // Specific response for empty or NULL queries
  void SendEmptyQueryResponse();

  /* Helper function used to make hardcoded ParameterStatus('S')
   * packets during startup
   */
  void MakeHardcodedParameterStatus(
      const std::pair<std::string, std::string>& kv);

  /* We don't support "SET" and "SHOW" SQL commands yet.
   * Also, duplicate BEGINs and COMMITs shouldn't be executed.
   * This function helps filtering out the execution for such cases
   */
  bool HardcodedExecuteFilter(QueryType query_type);

  /* Execute a Simple query protocol message */
  ProcessResult ExecQueryMessage(InputPacket* pkt, const size_t thread_id);

  /* Process the PARSE message of the extended query protocol */
  void ExecParseMessage(InputPacket* pkt);

  /* Process the BIND message of the extended query protocol */
  void ExecBindMessage(InputPacket* pkt);

  /* Process the DESCRIBE message of the extended query protocol */
  ProcessResult ExecDescribeMessage(InputPacket* pkt);

  /* Process the EXECUTE message of the extended query protocol */
  ProcessResult ExecExecuteMessage(InputPacket* pkt, const size_t thread_id);

  /* Process the optional CLOSE message of the extended query protocol */
  void ExecCloseMessage(InputPacket* pkt);

  void ExecExecuteMessageGetResult(ResultType status);

  void ExecQueryMessageGetResult(ResultType status);
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  NetworkProtocolType protocol_type_;

  // Manage standalone queries
  std::shared_ptr<Statement> unnamed_statement_;

  // The result-column format code
  std::vector<int> result_format_;

  // global txn state
  NetworkTransactionStateType txn_state_;

  // state to mang skipped queries
  bool skipped_stmt_ = false;
  std::string skipped_query_string_;
  QueryType skipped_query_type_;

  // Statement cache
  // StatementName -> Statement
  Cache<std::string, Statement> statement_cache_;
  // TableOid -> Statements
  // FIXME: This table statement cache is not in sync with the other cache.
  // That means if something gets thrown out of the statement cache it is not
  // automatically evicted from this cache.
  std::unordered_map<oid_t, std::vector<Statement*>> table_statement_cache_;

  //  Portals
  std::unordered_map<std::string, std::shared_ptr<Portal>> portals_;

  // packets ready for read
  size_t pkt_cntr_;

  // Manage parameter types for unnamed statement
  stats::QueryMetric::QueryParamBuf unnamed_stmt_param_types_;

  // Parameter types for statements
  // Warning: the data in the param buffer becomes invalid when the value
  // stored
  // in stat table is destroyed
  std::unordered_map<std::string, stats::QueryMetric::QueryParamBuf>
      statement_param_types_;

  //TODO: should this stay in traffic_cop?
  std::shared_ptr<Statement> statement_;

  std::string error_message_;

  //TODO: shoud this stay in traffic_cop?
  std::vector<StatementResult> results_;

  //TODO: should this stay in traffic_cop?
  int rows_affected_ = 0;

  //TODO: should this stay in traffic_cop?
  std::vector<type::Value> param_values_;

  //TODO: should this stay in traffic_cop?
  QueryType query_type_;

  //TODO: should this stay in traffic_cop?
  std::string query_;

  //===--------------------------------------------------------------------===//
  // STATIC DATA
  //===--------------------------------------------------------------------===//

  static const std::unordered_map<std::string, std::string>
      parameter_status_map_;

};

}  // namespace network
}  // namespace peloton
