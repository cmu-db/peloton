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
#include "expression/expression_util.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace tcop {

std::shared_ptr<Statement> Tcop::PrepareStatement(ClientProcessState &state,
                                                  const std::string &statement_name,
                                                  const std::string &query_string,
                                                  std::unique_ptr<parser::SQLStatementList> &&sql_stmt_list) {
  LOG_TRACE("Prepare Statement query: %s", query_string.c_str());

  // Empty statement
  // TODO (Tianyi) Read through the parser code to see if this is appropriate
  if (sql_stmt_list == nullptr || sql_stmt_list->GetNumStatements() == 0)
    // TODO (Tianyi) Do we need another query type called QUERY_EMPTY?
    return std::make_shared<Statement>(statement_name,
                                       QueryType::QUERY_INVALID,
                                       query_string,
                                       std::move(sql_stmt_list));

  StatementType stmt_type = sql_stmt_list->GetStatement(0)->GetType();
  QueryType query_type =
      StatementTypeToQueryType(stmt_type, sql_stmt_list->GetStatement(0));
  auto statement = std::make_shared<Statement>(statement_name,
                                               query_type,
                                               query_string,
                                               std::move(sql_stmt_list));

  // TODO(Tianyu): Issue #1441. Hopefully Tianyi will fix this in his later
  // refactor

  // We can learn transaction's states, BEGIN, COMMIT, ABORT, or ROLLBACK from
  // member variables, tcop_txn_state_. We can also get single-statement txn or
  // multi-statement txn from member variable single_statement_txn_
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // --multi-statements except BEGIN in a transaction
  if (!state.tcop_txn_state_.empty()) {
    state.single_statement_txn_ = false;
    // multi-statment txn has been aborted, just skip this query,
    // and do not need to parse or execute this query anymore.
    // Do not return nullptr in case that 'COMMIT' cannot be execute,
    // because nullptr will directly return ResultType::FAILURE to
    // packet_manager
    if (state.tcop_txn_state_.top().second == ResultType::ABORTED)
      return statement;
  } else {
    // Begin new transaction when received single-statement query or "BEGIN"
    // from multi-statement query
    if (statement->GetQueryType() ==
        QueryType::QUERY_BEGIN) {  // only begin a new transaction
      // note this transaction is not single-statement transaction
      LOG_TRACE("BEGIN");
      state.single_statement_txn_ = false;
    } else {
      // single statement
      LOG_TRACE("SINGLE TXN");
      state.single_statement_txn_ = true;
    }
    auto txn = txn_manager.BeginTransaction(state.thread_id_);
    // this shouldn't happen
    if (txn == nullptr) {
      LOG_TRACE("Begin txn failed");
    }
    // initialize the current result as success
    state.tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  if (settings::SettingsManager::GetBool(settings::SettingId::brain)) {
    state.tcop_txn_state_.top().first->AddQueryString(query_string.c_str());
  }

  // TODO(Tianyi) Move Statement Planing into Statement's method
  // to increase coherence
  try {
    // Run binder
    auto bind_node_visitor = binder::BindNodeVisitor(
        state.tcop_txn_state_.top().first, state.db_name_);
    bind_node_visitor.BindNameToNode(
        statement->GetStmtParseTreeList()->GetStatement(0));
    auto plan = state.optimizer_->BuildPelotonPlanTree(
        statement->GetStmtParseTreeList(), state.tcop_txn_state_.top().first);
    statement->SetPlanTree(plan);
    // Get the tables that our plan references so that we know how to
    // invalidate it at a later point when the catalog changes
    const std::set<oid_t> table_oids =
        planner::PlanUtil::GetTablesReferenced(plan.get());
    statement->SetReferencedTables(table_oids);

    if (query_type == QueryType::QUERY_SELECT) {
      auto tuple_descriptor = GenerateTupleDescriptor(state,
          statement->GetStmtParseTreeList()->GetStatement(0));
      statement->SetTupleDescriptor(tuple_descriptor);
      LOG_TRACE("select query, finish setting");
    }
  } catch (Exception &e) {
    state.error_message_ = e.what();
    tcop::Tcop::GetInstance().ProcessInvalidStatement(state);
    return nullptr;
  }

#ifdef LOG_DEBUG_ENABLED
  if (statement->GetPlanTree().get() != nullptr) {
    LOG_TRACE("Statement Prepared: %s", statement->GetInfo().c_str());
    LOG_TRACE("%s", statement->GetPlanTree().get()->GetInfo().c_str());
  }
#endif
  return statement;
}

ResultType Tcop::ExecuteStatement(ClientProcessState &state,
                                  CallbackFunc callback) {

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
      case QueryType::QUERY_BEGIN:return BeginQueryHelper(state);
      case QueryType::QUERY_COMMIT:return CommitQueryHelper(state);
      case QueryType::QUERY_ROLLBACK:return AbortQueryHelper(state);
      default:
        // The statement may be out of date
        // It needs to be replan
        if (state.statement_->GetNeedsReplan()) {
          // TODO(Tianyi) Move Statement Replan into Statement's method
          // to increase coherence
          auto bind_node_visitor = binder::BindNodeVisitor(
              state.tcop_txn_state_.top().first, state.db_name_);
          bind_node_visitor.BindNameToNode(
              state.statement_->GetStmtParseTreeList()->GetStatement(0));
          auto plan = state.optimizer_->BuildPelotonPlanTree(
              state.statement_->GetStmtParseTreeList(),
              state.tcop_txn_state_.top().first);
          state.statement_->SetPlanTree(plan);
          state.statement_->SetNeedsReplan(true);
        }

        ExecuteHelper(state, callback);
        if (state.is_queuing_)
          return ResultType::QUEUING;
        else
          return ExecuteStatementGetResult(state);
    }
  } catch (Exception &e) {
    state.error_message_ = e.what();
    return ResultType::FAILURE;
  }
}

bool Tcop::BindParamsForCachePlan(ClientProcessState &state,
                                  const std::vector<std::unique_ptr<expression::AbstractExpression>> &exprs) {
  if (state.tcop_txn_state_.empty()) {
    state.single_statement_txn_ = true;
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction(state.thread_id_);
    // this shouldn't happen
    if (txn == nullptr) {
      LOG_ERROR("Begin txn failed");
    }
    // initialize the current result as success
    state.tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }
  // Run binder
  auto bind_node_visitor =
      binder::BindNodeVisitor(state.tcop_txn_state_.top().first,
                              state.db_name_);

  std::vector<type::Value> param_values;
  for (const auto &expr :exprs) {
    if (!expression::ExpressionUtil::IsValidStaticExpression(expr.get())) {
      state.error_message_ = "Invalid Expression Type";
      return false;
    }
    expr->Accept(&bind_node_visitor);
    // TODO(Yuchen): need better check for nullptr argument
    param_values.push_back(expr->Evaluate(nullptr, nullptr, nullptr));
  }
  if (!param_values.empty()) {
    state.statement_->GetPlanTree()->SetParameterValues(&param_values);
  }
  state.param_values_ = param_values;
  return true;
}

std::vector<FieldInfo> Tcop::GenerateTupleDescriptor(ClientProcessState &state,
                                                     parser::SQLStatement *sql_stmt) {
  std::vector<FieldInfo> tuple_descriptor;
  if (sql_stmt->GetType() != StatementType::SELECT) return tuple_descriptor;
  auto select_stmt = (parser::SelectStatement *) sql_stmt;

  // TODO(Bowei): this is a hack which I don't have time to fix now
  // but it replaces a worse hack that was here before
  // What should happen here is that plan nodes should store
  // the schema of their expected results and here we should just read
  // it and put it in the tuple descriptor

  // Get the columns information and set up
  // the columns description for the returned results
  // Set up the table
  std::vector<catalog::Column> all_columns;

  // Check if query only has one Table
  // Example : SELECT * FROM A;
  GetTableColumns(state, select_stmt->from_table.get(), all_columns);

  int count = 0;
  for (auto &expr : select_stmt->select_list) {
    count++;
    if (expr->GetExpressionType() == ExpressionType::STAR) {
      for (const auto &column : all_columns) {
        tuple_descriptor.push_back(
            GetColumnFieldForValueType(column.GetName(), column.GetType()));
      }
    } else {
      std::string col_name;
      if (expr->alias.empty()) {
        col_name = expr->expr_name_.empty()
                   ? std::string("expr") + std::to_string(count)
                   : expr->expr_name_;
      } else {
        col_name = expr->alias;
      }
      tuple_descriptor.push_back(
          GetColumnFieldForValueType(col_name, expr->GetValueType()));
    }
  }

  return tuple_descriptor;
}

FieldInfo Tcop::GetColumnFieldForValueType(std::string column_name,
                                           type::TypeId column_type) {
  PostgresValueType field_type;
  size_t field_size;
  switch (column_type) {
    case type::TypeId::BOOLEAN:
    case type::TypeId::TINYINT: {
      field_type = PostgresValueType::BOOLEAN;
      field_size = 1;
      break;
    }
    case type::TypeId::SMALLINT: {
      field_type = PostgresValueType::SMALLINT;
      field_size = 2;
      break;
    }
    case type::TypeId::INTEGER: {
      field_type = PostgresValueType::INTEGER;
      field_size = 4;
      break;
    }
    case type::TypeId::BIGINT: {
      field_type = PostgresValueType::BIGINT;
      field_size = 8;
      break;
    }
    case type::TypeId::DECIMAL: {
      field_type = PostgresValueType::DOUBLE;
      field_size = 8;
      break;
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY: {
      field_type = PostgresValueType::TEXT;
      field_size = 255;
      break;
    }
    case type::TypeId::DATE: {
      field_type = PostgresValueType::DATE;
      field_size = 4;
      break;
    }
    case type::TypeId::TIMESTAMP: {
      field_type = PostgresValueType::TIMESTAMPS;
      field_size = 64;  // FIXME: Bytes???
      break;
    }
    default: {
      // Type not Identified
      LOG_ERROR("Unrecognized field type '%s' for field '%s'",
                TypeIdToString(column_type).c_str(), column_name.c_str());
      field_type = PostgresValueType::TEXT;
      field_size = 255;
      break;
    }
  }
  // HACK: Convert the type into a oid_t
  // This ugly and I don't like it one bit...
  return std::make_tuple(column_name, static_cast<oid_t>(field_type),
                         field_size);
}

void Tcop::GetTableColumns(ClientProcessState &state,
                           parser::TableRef *from_table,
                           std::vector<catalog::Column> &target_columns) {
  if (from_table == nullptr) return;

  // Query derived table
  if (from_table->select != nullptr) {
    for (auto &expr : from_table->select->select_list) {
      if (expr->GetExpressionType() == ExpressionType::STAR)
        GetTableColumns(state, from_table->select->from_table.get(), target_columns);
      else
        target_columns.emplace_back(expr->GetValueType(), 0,
                                    expr->GetExpressionName());
    }
  } else if (from_table->list.empty()) {
    if (from_table->join == nullptr) {
      auto columns =
          catalog::Catalog::GetInstance()->GetTableWithName(
              state.GetCurrentTxnState().first,
              from_table->GetDatabaseName(),
              from_table->GetSchemaName(),
              from_table->GetTableName())
              ->GetSchema()
              ->GetColumns();
      target_columns.insert(target_columns.end(), columns.begin(),
                            columns.end());
    } else {
      GetTableColumns(state, from_table->join->left.get(), target_columns);
      GetTableColumns(state, from_table->join->right.get(), target_columns);
    }
  }
    // Query has multiple tables. Recursively add all tables
  else
    for (auto &table : from_table->list)
      GetTableColumns(state, table.get(), target_columns);
}

void Tcop::ExecuteStatementPlanGetResult(ClientProcessState &state) {
  if (state.p_status_.m_result == ResultType::FAILURE) return;

  auto txn_result = state.GetCurrentTxnState().first->GetResult();
  if (state.single_statement_txn_ || txn_result == ResultType::FAILURE) {
    LOG_TRACE("About to commit/abort: single stmt: %d,txn_result: %s",
              state.single_statement_txn_,
              ResultTypeToString(txn_result).c_str());
    switch (txn_result) {
      case ResultType::SUCCESS:
        // Commit single statement
        LOG_TRACE("Commit Transaction");
        state.p_status_.m_result = CommitQueryHelper(state);
        break;
      case ResultType::FAILURE:
      default:
        // Abort
        LOG_TRACE("Abort Transaction");
        if (state.single_statement_txn_) {
          LOG_TRACE("Tcop_txn_state size: %lu", state.tcop_txn_state_.size());
          state.p_status_.m_result = AbortQueryHelper(state);
        } else {
          state.tcop_txn_state_.top().second = ResultType::ABORTED;
          state.p_status_.m_result = ResultType::ABORTED;
        }
    }
  }
}

ResultType Tcop::ExecuteStatementGetResult(ClientProcessState &state) {
  LOG_TRACE("Statement executed. Result: %s",
           ResultTypeToString(p_status_.m_result).c_str());
  state.rows_affected_ = state.p_status_.m_processed;
  LOG_TRACE("rows_changed %d", state.p_status_.m_processed);
  state.is_queuing_ = false;
  return state.p_status_.m_result;
}

void Tcop::ProcessInvalidStatement(ClientProcessState &state) {
  if (state.single_statement_txn_) {
    LOG_TRACE("SINGLE ABORT!");
    AbortQueryHelper(state);
  } else {  // multi-statment txn
    if (state.tcop_txn_state_.top().second != ResultType::ABORTED) {
      state.tcop_txn_state_.top().second = ResultType::ABORTED;
    }
  }
}

ResultType Tcop::CommitQueryHelper(ClientProcessState &state) {
// do nothing if we have no active txns
  if (state.tcop_txn_state_.empty()) return ResultType::NOOP;
  auto &curr_state = state.tcop_txn_state_.top();
  state.tcop_txn_state_.pop();
  auto txn = curr_state.first;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // I catch the exception (ex. table not found) explicitly,
  // If this exception is caused by a query in a transaction,
  // I will block following queries in that transaction until 'COMMIT' or
  // 'ROLLBACK' After receive 'COMMIT', see if it is rollback or really commit.
  if (curr_state.second != ResultType::ABORTED) {
    // txn committed
    return txn_manager.CommitTransaction(txn);
  } else {
    // otherwise, rollback
    return txn_manager.AbortTransaction(txn);
  }
}

ResultType Tcop::BeginQueryHelper(ClientProcessState &state) {
  if (state.tcop_txn_state_.empty()) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction(state.thread_id_);
    // this shouldn't happen
    if (txn == nullptr) {
      LOG_DEBUG("Begin txn failed");
      return ResultType::FAILURE;
    }
    // initialize the current result as success
    state.tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }
  return ResultType::SUCCESS;
}

ResultType Tcop::AbortQueryHelper(ClientProcessState &state) {
  // do nothing if we have no active txns
  if (state.tcop_txn_state_.empty()) return ResultType::NOOP;
  auto &curr_state = state.tcop_txn_state_.top();
  state.tcop_txn_state_.pop();
  // explicitly abort the txn only if it has not aborted already
  if (curr_state.second != ResultType::ABORTED) {
    auto txn = curr_state.first;
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto result = txn_manager.AbortTransaction(txn);
    return result;
  } else {
    delete curr_state.first;
    // otherwise, the txn has already been aborted
    return ResultType::ABORTED;
  }
}

executor::ExecutionResult Tcop::ExecuteHelper(ClientProcessState &state,
                                              CallbackFunc callback) {
  auto &curr_state = state.GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  if (!state.tcop_txn_state_.empty()) {
    txn = curr_state.first;
  } else {
    // No active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    state.single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(state.thread_id_);
    state.tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    // If the transaction state is ABORTED, the transaction should be aborted
    // but Peloton didn't explicitly abort it yet since it didn't receive a
    // COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken
    // transaction,
    // it should tell the client that these queries will not be executed.
    state.p_status_.m_result = ResultType::TO_ABORT;
    return state.p_status_;
  }

  auto on_complete = [callback, &state](executor::ExecutionResult p_status,
                         std::vector<ResultValue> &&values) {
    state.p_status_ = p_status;
    // TODO (Tianyi) I would make a decision on keeping one of p_status or
    // error_message in my next PR
    state.error_message_ = std::move(p_status.m_error_message);
    state.result_ = std::move(values);
    callback();
  };
  // TODO(Tianyu): Eliminate this copy, which is here to coerce the type
  std::vector<int> formats;
  for (auto format : state.result_format_)
    formats.push_back((int) format);

  auto &pool = threadpool::MonoQueuePool::GetInstance();
  pool.SubmitTask([on_complete, txn, formats, &state] {
    executor::PlanExecutor::ExecutePlan(state.statement_->GetPlanTree(),
                                        txn,
                                        state.param_values_,
                                        formats,
                                        on_complete);
  });

  state.is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu",
            state.tcop_txn_state_.size());
  return state.p_status_;
}

} // namespace tcop
} // namespace peloton