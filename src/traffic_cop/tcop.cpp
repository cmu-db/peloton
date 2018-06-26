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

#include "planner/plan_util.h"
#include "binder/bind_node_visitor.h"
#include "traffic_cop/tcop.h"

namespace peloton {
namespace tcop {

// Prepare a statement
bool tcop::PrepareStatement(
    ClientProcessState &state, const std::string &query_string,
    const std::string &statement_name) {
  try {
    // TODO(Tianyi) Implicitly start a txn

    // parse the query
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(query_string);

    // When the query is empty(such as ";" or ";;", still valid),
    // the parse tree is empty, parser will return nullptr.
    if (sql_stmt_list != nullptr && !sql_stmt_list->is_valid) {
      throw ParserException("Error Parsing SQL statement");
    }

    // TODO(Yuchen): Hack. We only process the first statement in the packet now.
    // We should store the rest of statements that will not be processed right
    // away. For the hack, in most cases, it works. Because for example in psql,
    // one packet contains only one query. But when using the pipeline mode in
    // Libpqxx, it sends multiple query in one packet. In this case, it's
    // incorrect.
    StatementType stmt_type = sql_stmt_list->GetStatement(0)->GetType();
    QueryType query_type =
        StatementTypeToQueryType(stmt_type, sql_stmt_list->GetStatement(0));

    std::shared_ptr<Statement> statement = std::make_shared<Statement>(
      statement_name, query_type, query_string, std::move(sql_stmt_list));

    // Empty statement edge case
    if (sql_stmt_list == nullptr ||
        sql_stmt_list->GetNumStatements() == 0) {
      std::shared_ptr<Statement> statement =
          std::make_shared<Statement>(statement_name, QueryType::QUERY_INVALID,
                                      query_string, std::move(sql_stmt_list));
      state.statement_cache_.AddStatement(statement);
      return true;
    }

    // Run binder
    auto bind_node_visitor = binder::BindNodeVisitor(
        tcop_txn_state_.top().first, state.db_name_);
    bind_node_visitor.BindNameToNode(
        statement->GetStmtParseTreeList()->GetStatement(0));
    auto plan = state.optimizer_->BuildPelotonPlanTree(
        statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);
    statement->SetPlanTree(plan);
    // Get the tables that our plan references so that we know how to
    // invalidate it at a later point when the catalog changes
    const std::set<oid_t> table_oids =
        planner::PlanUtil::GetTablesReferenced(plan.get());
    statement->SetReferencedTables(table_oids);

    if (query_type == QueryType::QUERY_SELECT) {
      auto tuple_descriptor = GenerateTupleDescriptor(
          statement->GetStmtParseTreeList()->GetStatement(0));
      statement->SetTupleDescriptor(tuple_descriptor);
      LOG_TRACE("select query, finish setting");
    }

    state.statement_cache_.AddStatement(statement);

  }  // If the statement is invalid or not supported yet
  catch (Exception &e) {
    // TODO implicit end the txn here
    state.error_message_ = e.what();
    return false;
  }

  return true;
}




} // namespace tcop
} // namespace peloton