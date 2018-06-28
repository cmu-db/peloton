//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcop.h
//
// Identification: src/include/traffic_cop/tcop.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "threadpool/mono_queue_pool.h"
#include "planner/plan_util.h"
#include "binder/bind_node_visitor.h"
#include "traffic_cop/tcop.h"

namespace peloton {
namespace tcop {

// Prepare a statement
bool tcop::PrepareStatement(ClientProcessState &state,
                            const std::string &query_string,
                            const std::string &statement_name) {
  try {
    state.txn_handle_.ImplicitBegin(state.thread_id_);
    // parse the query
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(query_string);

    // When the query is empty(such as ";" or ";;", still valid),
    // the parse tree is empty, parser will return nullptr.
    if (sql_stmt_list != nullptr && !sql_stmt_list->is_valid)
      throw ParserException("Error Parsing SQL statement");

    // TODO(Yuchen): Hack. We only process the first statement in the packet now.
    // We should store the rest of statements that will not be processed right
    // away. For the hack, in most cases, it works. Because for example in psql,
    // one packet contains only one query. But when using the pipeline mode in
    // Libpqxx, it sends multiple query in one packet. In this case, it's
    // incorrect.
    StatementType stmt_type = sql_stmt_list->GetStatement(0)->GetType();
    QueryType query_type =
        StatementTypeToQueryType(stmt_type, sql_stmt_list->GetStatement(0));

    auto statement = std::make_shared<Statement>(statement_name,
                                                 query_type,
                                                 query_string,
                                                 std::move(sql_stmt_list));

    // Empty statement edge case
    if (statement->GetStmtParseTreeList() == nullptr ||
        statement->GetStmtParseTreeList()->GetNumStatements() == 0) {
      state.statement_cache_.AddStatement(
          std::make_shared<Statement>(statement_name,
                                      QueryType::QUERY_INVALID,
                                      query_string,
                                      std::move(statement->PassStmtParseTreeList())));
      return true;
    }

    // Run binder
    auto bind_node_visitor = binder::BindNodeVisitor(state.txn_handle_.GetTxn(),
                                                     state.db_name_);
    bind_node_visitor.BindNameToNode(
        statement->GetStmtParseTreeList()->GetStatement(0));
    auto plan = state.optimizer_->
        BuildPelotonPlanTree(statement->GetStmtParseTreeList(),
                             state.txn_handle_.GetTxn());
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

  } catch (Exception &e) {
    // TODO(Tianyi) implicit end the txn here
    state.error_message_ = e.what();
    return false;
  }
  // TODO(Tianyi) catch txn exception
  return true;
}

bool tcop::ExecuteStatement(ClientProcessState &state,
                            const std::vector<int> &result_format,
                            std::vector<ResultValue> &result,
                            const CallbackFunc &callback) {

  LOG_TRACE("Execute Statement of name: %s",
            state.statement_->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            state.statement_->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(state.statement_->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s",
            state.statement_->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------",
            static_cast<int>(state.statement_->GetQueryType()));

  try {
    switch (state.statement_->GetQueryType()) {
      case QueryType::QUERY_BEGIN: {
        state.txn_handle_.ExplicitBegin(state.thread_id_);
        return true;
      }
      case QueryType::QUERY_COMMIT: {
        if (!state.txn_handle_.ExplicitCommit()) {
          state.p_status_.m_result = ResultType::FAILURE;
          //TODO set error message
        }
        return true;
      }
      case QueryType::QUERY_ROLLBACK: {
        state.txn_handle_.ExplicitAbort();
        return true;
      }
      default: {
        // The statement may be out of date
        // It needs to be replan
        auto txn = state.txn_handle_.ImplicitBegin(state.thread_id_);
        if (state.statement_->GetNeedsReplan()) {
          // TODO(Tianyi) Move Statement Replan into Statement's method
          // to increase coherence
          auto plan = state.optimizer_->BuildPelotonPlanTree(
              state.statement_->GetStmtParseTreeList(), txn);
          state.statement_->SetPlanTree(plan);
          state.statement_->SetNeedsReplan(false);
        }

        auto plan = state.statement_->GetPlanTree();
        auto params = state.param_values_;

        auto on_complete = [callback, &](executor::ExecutionResult p_status,
                                         std::vector<ResultValue> &&values) {
          state.p_status_ = p_status;
          state.error_message_ = std::move(p_status.m_error_message);
          result = std::move(values);
          callback();
        };

        auto &pool = threadpool::MonoQueuePool::GetInstance();
        pool.SubmitTask([txn, on_complete, &] {
          executor::PlanExecutor::ExecutePlan(state.statement_->GetPlanTree(),
                                              txn,
                                              state.param_values_,
                                              result_format,
                                              on_complete);
        });
        return false;
      }
    }
  } catch (Exception &e) {
    state.p_status_.m_result = ResultType::FAILURE;
    state.error_message_ = e.what();
    return true;
  }
}

} // namespace tcop
} // namespace peloton