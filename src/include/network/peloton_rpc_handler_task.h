//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton_rpc_handler_task.h
//
// Identification: src/include/network/peloton_rpc_handler_task.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <random>
#include "capnp/ez-rpc.h"
#include "capnp/message.h"
#include "catalog/catalog.h"
#include "common/dedicated_thread_task.h"
#include "common/logger.h"
#include "common/internal_types.h"
#include "kj/debug.h"
#include "peloton/capnp/peloton_service.capnp.h"
#include "codegen/buffering_consumer.h"
#include "executor/executor_context.h"
#include "planner/populate_index_plan.h"
#include "storage/storage_manager.h"
#include "planner/seq_scan_plan.h"
#include "catalog/system_catalogs.h"
#include "catalog/column_catalog.h"
#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/plan_executor.h"
#include "gmock/gtest/gtest.h"
#include "optimizer/optimizer.h"
#include "optimizer/rule.h"
#include "parser/postgresparser.h"
#include "planner/plan_util.h"
#include "optimizer/stats/stats_storage.h"
#include "traffic_cop/traffic_cop.h"

namespace peloton {
namespace network {
class PelotonRpcServerImpl final : public PelotonService::Server {
 private:
  static std::atomic_int counter_;

 protected:
  kj::Promise<void> dropIndex(DropIndexContext request) override {
    auto database_oid = request.getParams().getRequest().getDatabaseOid();
    auto index_oid = request.getParams().getRequest().getIndexOid();
    LOG_TRACE("Database oid: %d", database_oid);
    LOG_TRACE("Index oid: %d", index_oid);

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    // Drop index. Fail if it doesn't exist.
    auto catalog = catalog::Catalog::GetInstance();
    try {
      catalog->DropIndex(database_oid, index_oid, txn);
    } catch (CatalogException e) {
      LOG_ERROR("Drop Index Failed");
      txn_manager.AbortTransaction(txn);
      return kj::NEVER_DONE;
    }
    txn_manager.CommitTransaction(txn);
    return kj::READY_NOW;
  }

  kj::Promise<void> createIndex(CreateIndexContext request) override {
    LOG_DEBUG("Received RPC to create index");

    auto database_oid = request.getParams().getRequest().getDatabaseOid();
    auto table_oid = request.getParams().getRequest().getTableOid();
    auto col_oids = request.getParams().getRequest().getKeyAttrOids();
    auto index_name = request.getParams().getRequest().getIndexName();

    std::vector<oid_t> col_oid_vector;
    LOG_DEBUG("Database oid: %d", database_oid);
    LOG_DEBUG("Table oid: %d", table_oid);
    for (auto col : col_oids) {
      LOG_DEBUG("Col oid: %d", col);
      col_oid_vector.push_back(col);
    }

    // Create transaction to query the catalog.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    // Get the existing table so that we can find its oid and the cols oids.
    std::shared_ptr<catalog::TableCatalogObject> table_object;
    try {
      table_object = catalog::Catalog::GetInstance()->GetTableObject(
          database_oid, table_oid, txn);
    } catch (CatalogException e) {
      LOG_ERROR("Exception ocurred while getting table: %s",
                e.GetMessage().c_str());
      PELOTON_ASSERT(false);
    }

    auto table_name = table_object->GetTableName();
    auto col_obj_pairs = table_object->GetColumnObjects();

    // Done with the transaction.
    txn_manager.CommitTransaction(txn);

    // Get all the column names from the oids.
    std::vector<std::string> column_names;
    for (auto col_oid : col_oid_vector) {
      auto found_itr = col_obj_pairs.find(col_oid);
      if (found_itr != col_obj_pairs.end()) {
        auto col_obj = found_itr->second;
        column_names.push_back(col_obj->GetColumnName());
      } else {
        PELOTON_ASSERT(false);
      }
    }

    // Create "CREATE INDEX" query.
    std::ostringstream oss;
    oss << "CREATE INDEX " << index_name.cStr() << " ON ";
    oss << table_name << "(";
    for (auto i = 0UL; i < column_names.size(); i++) {
      oss << column_names[i];
      if (i < (column_names.size() - 1)) {
        oss << ",";
      }
    }
    oss << ")";

    LOG_DEBUG("Executing Create Index Query: %s", oss.str().c_str());

    // Execute the SQL query
    std::vector<ResultValue> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_affected;

    ExecuteSQLQuery(oss.str(), result, tuple_descriptor, rows_affected,
                    error_message);
    LOG_INFO("Execute query done");

    return kj::READY_NOW;
  }

  static void UtilTestTaskCallback(void *arg) {
    std::atomic_int *count = static_cast<std::atomic_int *>(arg);
    count->store(0);
  }

  // TODO: Avoid using this function.
  // Copied from SQL testing util.
  // Execute a SQL query end-to-end
  ResultType ExecuteSQLQuery(const std::string query,
                             std::vector<ResultValue> &result,
                             std::vector<FieldInfo> &tuple_descriptor,
                             int &rows_changed, std::string &error_message) {
    std::atomic_int counter_;

    LOG_INFO("Query: %s", query.c_str());
    // prepareStatement
    std::string unnamed_statement = "unnamed";
    auto &peloton_parser = parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(query);
    PELOTON_ASSERT(sql_stmt_list);
    if (!sql_stmt_list->is_valid) {
      return ResultType::FAILURE;
    }

    tcop::TrafficCop traffic_cop_(UtilTestTaskCallback, &counter_);

    auto statement = traffic_cop_.PrepareStatement(unnamed_statement, query,
                                                   std::move(sql_stmt_list));
    if (statement.get() == nullptr) {
      traffic_cop_.setRowsAffected(0);
      rows_changed = 0;
      error_message = traffic_cop_.GetErrorMessage();
      return ResultType::FAILURE;
    }
    // Execute Statement
    std::vector<type::Value> param_values;
    bool unnamed = false;
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
    // SetTrafficCopCounter();
    counter_.store(1);
    auto status = traffic_cop_.ExecuteStatement(
        statement, param_values, unnamed, nullptr, result_format, result);
    if (traffic_cop_.GetQueuing()) {
      while (counter_.load() == 1) {
        usleep(10);
      }
      traffic_cop_.ExecuteStatementPlanGetResult();
      status = traffic_cop_.ExecuteStatementGetResult();
      traffic_cop_.SetQueuing(false);
    }
    if (status == ResultType::SUCCESS) {
      tuple_descriptor = statement->GetTupleDescriptor();
    }
    LOG_INFO("Statement executed. Result: %s",
             ResultTypeToString(status).c_str());
    rows_changed = traffic_cop_.getRowsAffected();
    return status;
  }
};

class PelotonRpcHandlerTask : public DedicatedThreadTask {
 public:
  explicit PelotonRpcHandlerTask(const char *address) : address_(address) {}

  void Terminate() override {
    // TODO(tianyu): Not implemented. See:
    // https://groups.google.com/forum/#!topic/capnproto/bgxCdqGD6oE
  }

  void RunTask() override {
    capnp::EzRpcServer server(kj::heap<PelotonRpcServerImpl>(), address_);
    LOG_DEBUG("Server listening on %s", address_);
    kj::NEVER_DONE.wait(server.getWaitScope());
  }

 private:
  const char *address_;
};
}  // namespace network
}  // namespace peloton
