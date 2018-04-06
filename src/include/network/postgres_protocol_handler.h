//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_protocol_handler.h
//
// Identification: src/include/network/postgres_protocol_handler.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <boost/assign/list_of.hpp>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/cache.h"
#include "common/internal_types.h"
#include "common/portal.h"
#include "common/statement.h"
#include "common/statement_cache.h"
#include "protocol_handler.h"
#include "traffic_cop/traffic_cop.h"

// Packet content macros
#define NULL_CONTENT_SIZE -1

namespace peloton {

namespace parser {
class ExplainStatement;
}  // namespace parser

namespace network {

typedef std::vector<std::unique_ptr<OutputPacket>> ResponseBuffer;

class PostgresProtocolHandler : public ProtocolHandler {
 public:
  PostgresProtocolHandler(tcop::TrafficCop *traffic_cop);

  ~PostgresProtocolHandler();
  /**
   * Parse the content in the buffer and process to generate results.
   * @param rbuf The read buffer of network
   * @param thread_id The thread of current running thread. This is used
   * to generate txn
   * @return  @see ProcessResult
   */
  ProcessResult Process(Buffer &rbuf, size_t thread_id);

  // Deserialize the parame types from packet
  static size_t ReadParamType(InputPacket *pkt, int num_params,
                              std::vector<int32_t> &param_types);

  // Deserialize the parameter format from packet
  static size_t ReadParamFormat(InputPacket *pkt, int num_params_format,
                                std::vector<int16_t> &formats);

  // Deserialize the parameter value from packet
  static size_t ReadParamValue(
      InputPacket *pkt, int num_params, std::vector<int32_t> &param_types,
      std::vector<std::pair<type::TypeId, std::string>> &bind_parameters,
      std::vector<type::Value> &param_values, std::vector<int16_t> &formats);

  void Reset();

  void GetResult();

 private:
  //===--------------------------------------------------------------------===//
  // STATIC HELPERS
  //===--------------------------------------------------------------------===//

  /**
   * @brief Parse the input packet from rbuf
   * @param rbuf network read buffer
   * @param rpkt the postgres rpkt we want to parse to
   * @param startup_format whether we want the rpkt to be of startup packet
   * format
   *        (i.e. no type byte)
   * @return true if the parsing is complete
   */
  static bool ParseInputPacket(Buffer &rbuf, InputPacket &rpkt,
                               bool startup_format);

  /**
   * @brief Helper function to extract the body of Postgres packet from the
   * read buffer
   * @param rbuf network read buffer
   * @param rpkt the postgres rpkt we want to parse to
   * @return true if the parsing is complete
   */
  static bool ReadPacket(Buffer &rbuf, InputPacket &rpkt);

  /**
   * @brief Helper function to extract the header of a Postgres packet from the
   * read buffer
   * @see ParseInputPacket from param and return value
   */
  static bool ReadPacketHeader(Buffer &rbuf, InputPacket &rpkt,
                               bool startup_format);

  //===--------------------------------------------------------------------===//
  // PROTOCOL HANDLING FUNCTIONS
  //===--------------------------------------------------------------------===//

  /**
   * @brief Routine to deal with the first packet from the client
   */
  ProcessResult ProcessInitialPacket(InputPacket *pkt);

  /**
   * @brief Main Switch function to process general packets
   */
  ProcessResult ProcessNormalPacket(InputPacket *pkt, const size_t thread_id);

  /**
   * @brief Helper function to process startup packet
   * @param proto_version protocol version of the session
   */
  ProcessResult ProcessStartupPacket(InputPacket *pkt, int32_t proto_version);

  /**
   * Send hardcoded response
   */
  void SendStartupResponse();

  // Generic error protocol packet
  void SendErrorResponse(
      std::vector<std::pair<NetworkMessageType, std::string>> error_status);

  // Sends ready for query packet to the frontend
  void SendReadyForQuery(NetworkTransactionStateType txn_status);

  // Sends the attribute headers required by SELECT queries
  void PutTupleDescriptor(const std::vector<FieldInfo> &tuple_descriptor);

  // Send each row, one packet at a time, used by SELECT queries
  void SendDataRows(std::vector<ResultValue> &results, int colcount);

  // Used to send a packet that indicates the completion of a query. Also has
  // txn state mgmt
  void CompleteCommand(const QueryType &query_type, int rows);

  // Specific response for empty or NULL queries
  void SendEmptyQueryResponse();

  /* Helper function used to make hardcoded ParameterStatus('S')
   * packets during startup
   */
  void MakeHardcodedParameterStatus(
      const std::pair<std::string, std::string> &kv);

  /* We don't support "SET" and "SHOW" SQL commands yet.
   * Also, duplicate BEGINs and COMMITs shouldn't be executed.
   * This function helps filtering out the execution for such cases
   */
  bool HardcodedExecuteFilter(QueryType query_type);

  /* Execute a Simple query protocol message */
  ProcessResult ExecQueryMessage(InputPacket *pkt, const size_t thread_id);

  /* Execute a EXPLAIN query message */
  ResultType ExecQueryExplain(const std::string &query,
                              parser::ExplainStatement &explain_stmt);

  /* Process the PARSE message of the extended query protocol */
  void ExecParseMessage(InputPacket *pkt);

  /* Process the BIND message of the extended query protocol */
  void ExecBindMessage(InputPacket *pkt);

  /* Process the DESCRIBE message of the extended query protocol */
  ProcessResult ExecDescribeMessage(InputPacket *pkt);

  /* Process the EXECUTE message of the extended query protocol */
  ProcessResult ExecExecuteMessage(InputPacket *pkt, const size_t thread_id);

  /* Process the optional CLOSE message of the extended query protocol */
  void ExecCloseMessage(InputPacket *pkt);

  void ExecExecuteMessageGetResult(ResultType status);

  void ExecQueryMessageGetResult(ResultType status);

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//
  // True if this protocol is handling startup/SSL packets
  bool init_stage_;

  NetworkProtocolType protocol_type_;

  // Manage standalone queries

  // The result-column format code
  std::vector<int> result_format_;

  // global txn state
  NetworkTransactionStateType txn_state_;

  // state to manage skipped queries
  bool skipped_stmt_ = false;
  std::string skipped_query_string_;
  QueryType skipped_query_type_;

  // Statement cache
  StatementCache statement_cache_;

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

  std::unordered_map<std::string, std::string> cmdline_options_;

  //===--------------------------------------------------------------------===//
  // STATIC DATA
  //===--------------------------------------------------------------------===//

  static const std::unordered_map<std::string, std::string>
      parameter_status_map_;
};

}  // namespace network
}  // namespace peloton
