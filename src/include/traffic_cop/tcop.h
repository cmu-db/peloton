//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// t=cop.h
//
// Identification: src/include/traffic_cop/tcop.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include "network/connection_handle.h"
#include "parser/postgresparser.h"
#include "parser/sql_statement.h"
namespace peloton {
namespace tcop {

// TODO(Tianyu): Probably need a better name
// TODO(Tianyu): We can probably get rid of a bunch of fields from here
struct ClientProcessState {
  bool is_queuing_;
  std::string error_message_, db_name_ = DEFAULT_DB_NAME;
  std::vector<type::Value> param_values_;
  std::vector<ResultValue> results_;
  // This save currnet statement in the traffic cop
  std::shared_ptr<Statement> statement_;
  // Default database name
  int rows_affected_;
  // The optimizer used for this connection
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer_;
  // flag of single statement txn
  bool single_statement_txn_;
  std::vector<ResultValue> result_;
  network::ConnectionHandle conn_handle_;
};

inline std::unique_ptr<parser::SQLStatementList> ParseQuery(ClientProcessState state,
                                                            const std::string &query) {
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  // TODO(Tianyu): Parser result seems undocumented and I cannot tell
  // at a glance what any of these mean
  auto result = peloton_parser.BuildParseTree(query);
  if (result != nullptr && !result->is_valid)
    throw ParserException("Error parsing SQL statement");
  return std::move(result);
}

std::shared_ptr<Statement> PrepareStatement(ClientProcessState state,
                                            const std::string &stmt_name,
                                            const std::string &query_string,
                                            std::unique_ptr<parser::SQLStatementList> sql_stmt_list) {
  if (sql_stmt_list == nullptr || sql_stmt_list->GetNumStatements() == 0)
    return std::make_shared<Statement>(stmt_name,
                                       QueryType::QUERY_INVALID,
                                       query_string, std::move(sql_stmt_list));

}

} // namespace tcop
} // namespace peloton