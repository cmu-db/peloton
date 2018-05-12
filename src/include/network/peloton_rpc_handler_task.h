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
#include "capnp/ez-rpc.h"
#include "capnp/message.h"
#include "catalog/catalog.h"
#include "common/dedicated_thread_task.h"
#include "common/logger.h"
#include "common/internal_types.h"
#include "kj/debug.h"
#include "peloton/capnp/peloton_service.capnp.h"
#include "concurrency/transaction_manager_factory.h"
#include "codegen/buffering_consumer.h"
#include "executor/executor_context.h"
#include "codegen/buffering_consumer.h"
#include "codegen/proxy/string_functions_proxy.h"
#include "codegen/query.h"
#include "codegen/query_cache.h"
#include "codegen/query_compiler.h"
#include "codegen/type/decimal_type.h"
#include "codegen/type/integer_type.h"
#include "codegen/type/type.h"
#include "codegen/value.h"
#include "planner/populate_index_plan.h"
#include "traffic_cop/traffic_cop.h"
#include "storage/storage_manager.h"
#include "planner/seq_scan_plan.h"

namespace peloton {
namespace network {
class PelotonRpcServerImpl final : public PelotonService::Server {
 protected:
  kj::Promise<void> dropIndex(DropIndexContext request) override {
    auto database_oid = request.getParams().getRequest().getDatabaseOid();
    auto index_oid = request.getParams().getRequest().getIndexOid();
    LOG_DEBUG("Database oid: %d", database_oid);
    LOG_DEBUG("Index oid: %d", index_oid);

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
    auto is_unique = request.getParams().getRequest().getUniqueKeys();
    auto index_name = request.getParams().getRequest().getIndexName();

    std::vector<oid_t> col_oid_vector;
    LOG_DEBUG("Database oid: %d", database_oid);
    LOG_DEBUG("Table oid: %d", table_oid);
    for (auto col : col_oids) {
      LOG_DEBUG("Col oid: %d", col);
      col_oid_vector.push_back(col);
    }

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    // Create index. Fail if it already exists.
    auto catalog = catalog::Catalog::GetInstance();
    try {
      catalog->CreateIndex(database_oid, table_oid, col_oid_vector,
                           DEFUALT_SCHEMA_NAME, index_name, IndexType::BWTREE,
                           IndexConstraintType::DEFAULT, is_unique, txn);
    } catch (CatalogException e) {
      LOG_ERROR("Create Index Failed: %s", e.GetMessage().c_str());
      // TODO [vamshi]: Do we commit or abort?
      txn_manager.CommitTransaction(txn);
      return kj::READY_NOW;
    }

    // TODO [vamshi]: Hack change this.
    // Index created. Populate it.
    auto storage_manager = storage::StorageManager::GetInstance();
    auto table_object =
        storage_manager->GetTableWithOid(database_oid, table_oid);

    // Create a seq plan to retrieve data
    std::unique_ptr<planner::SeqScanPlan> populate_seq_plan(
      new planner::SeqScanPlan(table_object, nullptr, col_oid_vector, false));

    // Create a index plan
    std::shared_ptr<planner::AbstractPlan> populate_index_plan(
      new planner::PopulateIndexPlan(table_object, col_oid_vector));
    populate_index_plan->AddChild(std::move(populate_seq_plan));

    std::vector<type::Value> params;
    std::vector<ResultValue> result;
    std::atomic_int counter;
    std::vector<int> result_format;

    auto callback = [](void *arg) {
      std::atomic_int *count = static_cast<std::atomic_int *>(arg);
      count->store(0);
    };

    // Set the callback and context state.
    auto &traffic_cop = tcop::TrafficCop::GetInstance();
    traffic_cop.SetTaskCallback(callback, &counter);
    traffic_cop.SetTcopTxnState(txn);

    // Execute the plan through the traffic cop so that it runs on a separate
    // thread and we don't have to wait for the output.
    executor::ExecutionResult status = traffic_cop.ExecuteHelper(
        populate_index_plan, params, result, result_format);

    if (traffic_cop.GetQueuing()) {
      while (counter.load() == 1) {
        usleep(10);
      }
      if (traffic_cop.p_status_.m_result == ResultType::SUCCESS) {
        LOG_INFO("Index populate succeeded");
      } else {
        LOG_ERROR("Index populate failed");
      }
      traffic_cop.SetQueuing(false);
    }
    traffic_cop.CommitQueryHelper();

    return kj::READY_NOW;
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
