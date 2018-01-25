//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// traffic_cop.cpp
//
// Identification: src/traffic_cop/traffic_cop.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "traffic_cop/traffic_cop.h"

#include <utility>

#include "binder/bind_node_visitor.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "common/internal_types.h"
#include "expression/expression_util.h"
#include "optimizer/optimizer.h"
#include "planner/plan_util.h"
#include "settings/settings_manager.h"
#include "threadpool/mono_queue_pool.h"

namespace peloton {
namespace tcop {

TrafficCop::TrafficCop()
    : is_queuing_(false),
      rows_affected_(0),
      optimizer_(new optimizer::Optimizer()),
      single_statement_txn_(true) {}

TrafficCop::TrafficCop(void (*task_callback)(void *), void *task_callback_arg)
    : optimizer_(new optimizer::Optimizer()),
      single_statement_txn_(true),
      task_callback_(task_callback),
      task_callback_arg_(task_callback_arg) {}

void TrafficCop::Reset() {
  std::stack<TcopTxnState> new_tcop_txn_state;
  // clear out the stack
  swap(tcop_txn_state_, new_tcop_txn_state);
  optimizer_->Reset();
  results_.clear();
  param_values_.clear();
  setRowsAffected(0);
}

TrafficCop::~TrafficCop() {
  // Abort all running transactions
  while (!tcop_txn_state_.empty()) {
    AbortQueryHelper();
  }
}

/* Singleton accessor
 * NOTE: Used by in unit tests ONLY
 */
TrafficCop &TrafficCop::GetInstance() {
  static TrafficCop tcop;
  tcop.Reset();
  return tcop;
}

TrafficCop::TcopTxnState &TrafficCop::GetDefaultTxnState() {
  static TcopTxnState default_state;
  default_state = std::make_pair(nullptr, ResultType::INVALID);
  return default_state;
}

TrafficCop::TcopTxnState &TrafficCop::GetCurrentTxnState() {
  if (tcop_txn_state_.empty()) {
    return GetDefaultTxnState();
  }
  return tcop_txn_state_.top();
}

ResultType TrafficCop::BeginQueryHelper(size_t thread_id) {
  if (tcop_txn_state_.empty()) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction(thread_id);
    // this shouldn't happen
    if (txn == nullptr) {
      LOG_DEBUG("Begin txn failed");
      return ResultType::FAILURE;
    }
    // initialize the current result as success
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }
  return ResultType::SUCCESS;
}

ResultType TrafficCop::CommitQueryHelper() {
  // do nothing if we have no active txns
  if (tcop_txn_state_.empty()) return ResultType::NOOP;
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
  auto txn = curr_state.first;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // I catch the exception (ex. table not found) explicitly,
  // If this exception if caused by a query in a transaction,
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

ResultType TrafficCop::AbortQueryHelper() {
  // do nothing if we have no active txns
  if (tcop_txn_state_.empty()) return ResultType::NOOP;
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
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

ResultType TrafficCop::ExecuteStatementGetResult() {
  LOG_TRACE("Statement executed. Result: %s",
            ResultTypeToString(p_status_.m_result).c_str());
  setRowsAffected(p_status_.m_processed);
  LOG_TRACE("rows_changed %d", p_status_.m_processed);
  is_queuing_ = false;
  return p_status_.m_result;
}

/*
 * Execute a statement that needs a plan(so, BEGIN, COMMIT, ROLLBACK does not
 * come here).
 * Begin a new transaction if necessary.
 * If the current transaction is already broken(for example due to previous
 * invalid
 * queries), directly return
 * Otherwise, call ExecutePlan()
 */
executor::ExecutionResult TrafficCop::ExecuteHelper(
    std::shared_ptr<planner::AbstractPlan> plan,
    const std::vector<type::Value> &params, std::vector<ResultValue> &result,
    const std::vector<int> &result_format, size_t thread_id) {
  auto &curr_state = GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  if (!tcop_txn_state_.empty()) {
    txn = curr_state.first;
  } else {
    // No active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(thread_id);
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    // If the transaction state is ABORTED, the transaction should be aborted
    // but Peloton didn't explicitly abort it yet since it didn't receive a
    // COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken
    // transaction,
    // it should tell the client that these queries will not be executed.
    p_status_.m_result = ResultType::TO_ABORT;
    return p_status_;
  }

  auto on_complete = [&result, this](executor::ExecutionResult p_status,
                                     std::vector<ResultValue> &&values) {
    this->p_status_ = p_status;
    result = std::move(values);
    task_callback_(task_callback_arg_);
  };

  auto &pool = threadpool::MonoQueuePool::GetInstance();
  pool.SubmitTask([plan, txn, &params, &result, &result_format, on_complete] {
    executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format,
                                        on_complete);
  });

  is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu",
            tcop_txn_state_.size());
  return p_status_;
}

void TrafficCop::ExecuteStatementPlanGetResult() {
  bool init_failure = false;
  if (p_status_.m_result == ResultType::FAILURE) {
    // only possible if init failed
    init_failure = true;
  }

  auto txn_result = GetCurrentTxnState().first->GetResult();
  if (single_statement_txn_ || init_failure ||
      txn_result == ResultType::FAILURE) {
    LOG_TRACE(
        "About to commit: single stmt: %d, init_failure: %d, txn_result: %s",
        is_single_statement_txn_, init_failure,
        ResultTypeToString(txn_result).c_str());
    switch (txn_result) {
      case ResultType::SUCCESS:
        // Commit single statement
        LOG_TRACE("Commit Transaction");
        p_status_.m_result = CommitQueryHelper();
        break;

      case ResultType::FAILURE:
      default:
        // Abort
        LOG_TRACE("Abort Transaction");
        if (single_statement_txn_) {
          LOG_TRACE("Tcop_txn_state size: %lu", tcop_txn_state_.size());
          p_status_.m_result = AbortQueryHelper();
        } else {
          tcop_txn_state_.top().second = ResultType::ABORTED;
          p_status_.m_result = ResultType::ABORTED;
        }
    }
  }
}

/*
 * Prepare a statement based on parse tree. Begin a transaction if necessary.
 * If the query is not issued in a transaction (if txn_stack is empty and it's
 * not
 * BEGIN query), Peloton will create a new transation for it. single_stmt
 * transaction.
 * Otherwise, it's a multi_stmt transaction.
 * TODO(Yuchen): We do not need a query string to prepare a statement and the
 * query string may
 * contain the information of multiple statements rather than the single one.
 * Hack here. We store
 * the query string inside Statement objects for printing infomation.
 */
std::shared_ptr<Statement> TrafficCop::PrepareStatement(
    const std::string &stmt_name, const std::string &query_string,
    std::unique_ptr<parser::SQLStatementList> sql_stmt_list,
    UNUSED_ATTRIBUTE std::string &error_message,
    const size_t thread_id UNUSED_ATTRIBUTE) {
  LOG_TRACE("Prepare Statement query: %s", query_string.c_str());
  StatementType stmt_type = sql_stmt_list->GetStatement(0)->GetType();
  QueryType query_type =
      StatementTypeToQueryType(stmt_type, sql_stmt_list->GetStatement(0));
  std::shared_ptr<Statement> statement = std::make_shared<Statement>(
      stmt_name, query_type, query_string, std::move(sql_stmt_list));

  // We can learn transaction's states, BEGIN, COMMIT, ABORT, or ROLLBACK from
  // member variables, tcop_txn_state_. We can also get single-statement txn or
  // multi-statement txn from member variable single_statement_txn_
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // --multi-statements except BEGIN in a transaction
  if (!tcop_txn_state_.empty()) {
    single_statement_txn_ = false;
    // multi-statment txn has been aborted, just skip this query,
    // and do not need to parse or execute this query anymore.
    // Do not return nullptr in case that 'COMMIT' cannot be execute,
    // because nullptr will directly return ResultType::FAILURE to
    // packet_manager
    if (tcop_txn_state_.top().second == ResultType::ABORTED) {
      return statement;
    }
  } else {
    // Begin new transaction when received single-statement query or "BEGIN"
    // from multi-statement query
    if (statement->GetQueryType() ==
        QueryType::QUERY_BEGIN) {  // only begin a new transaction
      // note this transaction is not single-statement transaction
      LOG_TRACE("BEGIN");
      single_statement_txn_ = false;
    } else {
      // single statement
      LOG_TRACE("SINGLE TXN");
      single_statement_txn_ = true;
    }
    auto txn = txn_manager.BeginTransaction(thread_id);
    // this shouldn't happen
    if (txn == nullptr) {
      LOG_TRACE("Begin txn failed");
    }
    // initialize the current result as success
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  // TODO(Tianyi) Move Statement Planing into Statement's method
  // to increase coherence
  try {
    auto plan = optimizer_->BuildPelotonPlanTree(
        statement->GetStmtParseTreeList(), default_database_name_,
        tcop_txn_state_.top().first);
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
  } catch (Exception &e) {
    error_message = e.what();
    ProcessInvalidStatement();
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

/*
 * Do nothing if there is no active transaction;
 * If single-stmt transaction, abort it;
 * If multi-stmt transaction, just set transaction state to 'ABORTED'.
 * The multi-stmt txn will be explicitly aborted when receiving 'Commit' or
 * 'Rollback'.
 */
void TrafficCop::ProcessInvalidStatement() {
  if (single_statement_txn_) {
    LOG_TRACE("SINGLE ABORT!");
    AbortQueryHelper();
  } else {  // multi-statment txn
    if (tcop_txn_state_.top().second != ResultType::ABORTED) {
      tcop_txn_state_.top().second = ResultType::ABORTED;
    }
  }
}

bool TrafficCop::BindParamsForCachePlan(
    const std::vector<std::unique_ptr<expression::AbstractExpression>> &
        parameters,
    std::string &error_message, const size_t thread_id UNUSED_ATTRIBUTE) {
  if (tcop_txn_state_.empty()) {
    single_statement_txn_ = true;
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction(thread_id);
    // this shouldn't happen
    if (txn == nullptr) {
      LOG_ERROR("Begin txn failed");
    }
    // initialize the current result as success
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }
  // Run binder
  auto bind_node_visitor = std::make_shared<binder::BindNodeVisitor>(
      tcop_txn_state_.top().first, default_database_name_);

  std::vector<type::Value> param_values;
  for (const std::unique_ptr<expression::AbstractExpression> &param :
       parameters) {
    if (!expression::ExpressionUtil::IsValidStaticExpression(param.get())) {
      error_message = "Invalid Expression Type";
      return false;
    }
    param->Accept(bind_node_visitor.get());
    // TODO(Yuchen): need better check for nullptr argument
    param_values.push_back(param->Evaluate(nullptr, nullptr, nullptr));
  }
  if (param_values.size() > 0) {
    statement_->GetPlanTree()->SetParameterValues(&param_values);
  }
  SetParamVal(param_values);
  return true;
}

void TrafficCop::GetTableColumns(parser::TableRef *from_table,
                                 std::vector<catalog::Column> &target_columns) {
  if (from_table == nullptr) return;

  // Query derived table
  if (from_table->select != NULL) {
    for (auto &expr : from_table->select->select_list) {
      if (expr->GetExpressionType() == ExpressionType::STAR)
        GetTableColumns(from_table->select->from_table.get(), target_columns);
      else
        target_columns.push_back(catalog::Column(expr->GetValueType(), 0,
                                                 expr->GetExpressionName()));
    }
  } else if (from_table->list.empty()) {
    if (from_table->join == NULL) {
      auto columns =
          static_cast<storage::DataTable *>(
              catalog::Catalog::GetInstance()->GetTableWithName(
                  from_table->GetDatabaseName(), from_table->GetTableName(),
                  GetCurrentTxnState().first))
              ->GetSchema()
              ->GetColumns();
      target_columns.insert(target_columns.end(), columns.begin(),
                            columns.end());
    } else {
      GetTableColumns(from_table->join->left.get(), target_columns);
      GetTableColumns(from_table->join->right.get(), target_columns);
    }
  }
  // Query has multiple tables. Recursively add all tables
  else {
    for (auto &table : from_table->list) {
      GetTableColumns(table.get(), target_columns);
    }
  }
}

std::vector<FieldInfo> TrafficCop::GenerateTupleDescriptor(
    parser::SQLStatement *sql_stmt) {
  std::vector<FieldInfo> tuple_descriptor;
  if (sql_stmt->GetType() != StatementType::SELECT) return tuple_descriptor;
  auto select_stmt = (parser::SelectStatement *)sql_stmt;

  // TODO: this is a hack which I don't have time to fix now
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
  GetTableColumns(select_stmt->from_table.get(), all_columns);

  int count = 0;
  for (auto &expr : select_stmt->select_list) {
    count++;
    if (expr->GetExpressionType() == ExpressionType::STAR) {
      for (auto column : all_columns) {
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

// TODO: move it to postgres_protocal_handler.cpp
FieldInfo TrafficCop::GetColumnFieldForValueType(std::string column_name,
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

ResultType TrafficCop::ExecuteStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE bool unnamed,
    std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    std::string &error_message, size_t thread_id) {
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(
        statement, std::move(param_stats));
  }

  LOG_TRACE("Execute Statement of name: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            statement->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s",
            statement->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------",
            static_cast<int>(statement->GetQueryType()));

  try {
    switch (statement->GetQueryType()) {
      case QueryType::QUERY_BEGIN: {
        return BeginQueryHelper(thread_id);
      }
      case QueryType::QUERY_COMMIT: {
        return CommitQueryHelper();
      }
      case QueryType::QUERY_ROLLBACK: {
        return AbortQueryHelper();
      }
      default:
        // The statement may be out of date
        // It needs to be replan
        if (statement->GetNeedsReplan()) {
          // TODO(Tianyi) Move Statement Replan into Statement's method
          // to increase coherence
          auto plan =
              optimizer_->BuildPelotonPlanTree(statement->GetStmtParseTreeList(), default_database_name_, tcop_txn_state_.top().first);
          statement->SetPlanTree(plan);
          statement->SetNeedsReplan(true);
        }
        
        ExecuteHelper(statement->GetPlanTree(), params, result,
                      result_format, thread_id);
        if (GetQueuing()) {
          return ResultType::QUEUING;
        } else {
          return ExecuteStatementGetResult();
        }
    }

  } catch (Exception &e) {
    error_message = e.what();
    return ResultType::FAILURE;
  }
}

}  // namespace tcop
}  // namespace peloton
