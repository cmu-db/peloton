//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wire.h
//
// Identification: src/include/wire/wire.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/assign/list_of.hpp>

#include "common/cache.h"
#include "common/portal.h"
#include "common/statement.h"
#include "wire/marshal.h"

// TXN state definitions
#define TXN_IDLE 'I'
#define TXN_BLOCK 'T'
#define TXN_FAIL 'E'

namespace peloton {
namespace wire {

typedef std::vector<std::unique_ptr<Packet>> ResponseBuffer;

class PacketManager {
  /* Note: The responses argument in every subsequent function
   * is used to batch all the generated packets ofr that unit */

  // Generic error protocol packet
  void SendErrorResponse(
      std::vector<std::pair<uchar, std::string>> error_status,
      ResponseBuffer& responses);

  // Sends ready for query packet to the frontend
  void SendReadyForQuery(uchar txn_status, ResponseBuffer& responses);

  // Sends the attribute headers required by SELECT queries
  void PutTupleDescriptor(const std::vector<FieldInfoType>& tuple_descriptor,
                          ResponseBuffer& responses);

  // Send each row, one packet at a time, used by SELECT queries
  void SendDataRows(std::vector<ResultType>& results, int colcount,
                    int& rows_affected, ResponseBuffer& responses);

  // Used to send a packet that indicates the completion of a query. Also has
  // txn state mgmt
  void CompleteCommand(const std::string& query_type, int rows,
                       ResponseBuffer& responses);

  // Specific response for empty or NULL queries
  void SendEmptyQueryResponse(ResponseBuffer& responses);

  /* Helper function used to make hardcoded ParameterStatus('S')
   * packets during startup
   */
  void MakeHardcodedParameterStatus(
      ResponseBuffer& responses, const std::pair<std::string, std::string>& kv);

  /* SQLite doesn't support "SET" and "SHOW" SQL commands.
   * Also, duplicate BEGINs and COMMITs shouldn't be executed.
   * This function helps filtering out the execution for such cases
   */
  bool HardcodedExecuteFilter(std::string query_type);

  /* Execute a Simple query protocol message */
  void ExecQueryMessage(InputPacket* pkt, ResponseBuffer& responses);

  /* Process the PARSE message of the extended query protocol */
  void ExecParseMessage(InputPacket* pkt, ResponseBuffer& responses);

  /* Process the BIND message of the extended query protocol */
  void ExecBindMessage(InputPacket* pkt, ResponseBuffer& responses);

  /* Process the DESCRIBE message of the extended query protocol */
  bool ExecDescribeMessage(InputPacket* pkt, ResponseBuffer& responses);

  /* Process the EXECUTE message of the extended query protocol */
  void ExecExecuteMessage(InputPacket* pkt, ResponseBuffer& response);

  /* closes the socket connection with the client */
//  void CloseClient();

 public:
  // Deserialize the parameter types from packet
  static size_t ReadParamType(Packet* pkt, int num_params,
                              std::vector<int32_t>& param_types);

  // Deserialize the parameter format from packet
  static size_t ReadParamFormat(Packet* pkt, int num_params_format,
                                std::vector<int16_t>& formats);

  // Deserialize the parameter value from packet
  static size_t ReadParamValue(
      Packet* pkt, int num_params, std::vector<int32_t>& param_types,
      std::vector<std::pair<int, std::string>>& bind_parameters,
      std::vector<common::Value>& param_values, std::vector<int16_t>& formats);

  Client client_;

  // Manage standalone queries
  std::shared_ptr<Statement> unnamed_statement_;

  // Manage parameter types for unnamed statement
  stats::QueryMetric::QueryParamBuf unnamed_stmt_param_types_;

  // The result-column format code
  std::vector<int> result_format_;

  // gloabl txn state
  uchar txn_state_;

  // state to mang skipped queries
  bool skipped_stmt_ = false;
  std::string skipped_query_string_;
  std::string skipped_query_type_;

  static const std::unordered_map<std::string, std::string>
      parameter_status_map_;

  // Statement cache
  Cache<std::string, Statement> statement_cache_;

  // Parameter types for statements
  // Warning: the data in the param buffer becomes invalid when the value stored
  // in stat table is destroyed
  std::unordered_map<std::string, stats::QueryMetric::QueryParamBuf>
      statement_param_types_;

  //  Portals
  std::unordered_map<std::string, std::shared_ptr<Portal>> portals_;
  // packets ready for read
  size_t pkt_cntr_;

  inline PacketManager()
      : txn_state_(TXN_IDLE), pkt_cntr_(0) {}

  /* Startup packet processing logic */
  bool ProcessStartupPacket(InputPacket* pkt, ResponseBuffer& responses);

  /* Main switch case wrapper to process every packet apart from the startup
   * packet. Avoid flushing the response for extended protocols. */
  bool ProcessPacket(InputPacket* pkt, ResponseBuffer& responses, bool& force_flush);

  /* Manage the startup packet */
  //  bool ManageStartupPacket();

    /* Manage subsequent packets */
  //  bool ManagePacket();
};

}  // End wire namespace
}  // End peloton namespace
