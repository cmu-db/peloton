//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// packet_manager.h
//
// Identification: src/include/wire/packet_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <boost/assign/list_of.hpp>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/cache.h"
#include "common/portal.h"
#include "common/statement.h"
#include "tcop/tcop.h"
#include "wire/marshal.h"

// Packet content macros
#define NULL_CONTENT_SIZE -1

namespace peloton {

namespace wire {

typedef std::vector<std::unique_ptr<OutputPacket>> ResponseBuffer;

class PacketManager {
 public:
  PacketManager();

  ~PacketManager();

  /* Routine to deal with the first packet from the client */
  int ProcessInitialPacket(InputPacket* pkt);

  /* Routine to deal with general Startup message */
  bool ProcessStartupPacket(InputPacket* pkt, int32_t proto_version);

  /* Routine to deal with SSL request message */
  bool ProcessSSLRequestPacket(InputPacket *pkt);

  /* Main switch case wrapper to process every packet apart from the startup
   * packet. Avoid flushing the response for extended protocols. */
  bool ProcessPacket(InputPacket* pkt, const size_t thread_id);

  /* Manage the startup packet */
  //  bool ManageStartupPacket();
  void Reset();

  // Returns a vector of all the PreparedStatements that this PacketManager has
  // that reference the given table id
  const std::vector<Statement*> GetPreparedStatements(oid_t table_id) {
    return table_statement_cache_[table_id];
  }

  void InvalidatePreparedStatements(oid_t table_id);

  // Ugh... this should not be here but we have no choice...
  void ReplanPreparedStatement(Statement* statement);

  // Check existence of statement in cache by name
  // Return true if exists
  bool ExistCachedStatement(std::string statement_name) {
    auto statement_cache_itr = statement_cache_.find(statement_name);
    return statement_cache_itr != statement_cache_.end();
  }

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
      std::vector<std::pair<int, std::string>>& bind_parameters,
      std::vector<type::Value>& param_values, std::vector<int16_t>& formats);

  static std::vector<PacketManager*> GetPacketManagers() {
    return (PacketManager::packet_managers_);
  }

  Client client_;

  // has the startup packet been received for this connection
  bool is_started = false;
  bool ssl_sent = false;

  // Should we send the buffered packets right away?
  bool force_flush = false;

  // TODO declare a response buffer pool so that we can reuse the responses
  // so that we don't have to new packet each time
  ResponseBuffer responses;

 private:
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
  void CompleteCommand(const std::string& query_type, int rows);

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
  bool HardcodedExecuteFilter(std::string query_type);

  /* Execute a Simple query protocol message */
  void ExecQueryMessage(InputPacket* pkt, const size_t thread_id);

  /* Process the PARSE message of the extended query protocol */
  void ExecParseMessage(InputPacket* pkt);

  /* Process the BIND message of the extended query protocol */
  void ExecBindMessage(InputPacket* pkt);

  /* Process the DESCRIBE message of the extended query protocol */
  bool ExecDescribeMessage(InputPacket* pkt);

  /* Process the EXECUTE message of the extended query protocol */
  void ExecExecuteMessage(InputPacket* pkt, const size_t thread_id);

  /* Process the optional CLOSE message of the extended query protocol */
  void ExecCloseMessage(InputPacket* pkt);

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // Manage standalone queries
  std::shared_ptr<Statement> unnamed_statement_;

  // The result-column format code
  std::vector<int> result_format_;

  // global txn state
  NetworkTransactionStateType txn_state_;

  // state to mang skipped queries
  bool skipped_stmt_ = false;
  std::string skipped_query_string_;
  std::string skipped_query_type_;

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

  // The traffic cop used for this connection
  std::unique_ptr<tcop::TrafficCop> traffic_cop_;

  //===--------------------------------------------------------------------===//
  // STATIC DATA
  //===--------------------------------------------------------------------===//

  static const std::unordered_map<std::string, std::string>
      parameter_status_map_;

  // HACK: Global list of PacketManager instances
  // We need this in order to reset statement caches when the catalog changes
  // We need to think of a more elegant solution for this
  static std::vector<PacketManager*> packet_managers_;
  static std::mutex packet_managers_mutex_;
};

}  // End wire namespace
}  // End peloton namespace
