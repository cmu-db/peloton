//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_suggestion_task.cpp
//
// Identification: src/brain/index_suggestion_task.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "include/brain/index_suggestion_task.h"
#include "catalog/query_history_catalog.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {

namespace brain {

// Interval in seconds.
struct timeval IndexSuggestionTask::interval {
  10, 0
};

uint64_t IndexSuggestionTask::last_timestamp = 0;

uint64_t IndexSuggestionTask::tuning_threshold = 10;

void IndexSuggestionTask::Task(BrainEnvironment *env) {
  (void)env;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("Started Index Suggestion Task");

  // Query the catalog for new queries.
  auto query_catalog = &catalog::QueryHistoryCatalog::GetInstance(txn);
  auto queries =
      query_catalog->GetQueryStringsAfterTimestamp(last_timestamp, txn);
  if (queries->size() > tuning_threshold) {
    LOG_INFO("Tuning threshold has crossed. Time to tune the DB!");
    // TODO 1)
    // This is optional.
    // Validate the queries -- if they belong to any live tables in the
    // database.

    // TODO 2)
    // Run the index selection.
    // Create RPC for index creation on the server side.

    // TODO 3)
    // Update the last_timestamp to the be the latest query's timestamp in
    // the current workload, so that we fetch the new queries next time.
  } else {
    LOG_INFO("Tuning - not this time");
  }
  txn_manager.CommitTransaction(txn);
}

void IndexSuggestionTask::SendIndexCreateRPCToServer(std::string table_name,
                                                     std::vector<oid_t> keys) {
  // TODO: Remove hardcoded database name and server end point.
  capnp::EzRpcClient client("localhost:15445");
  PelotonService::Client peloton_service = client.getMain<PelotonService>();
  auto request = peloton_service.createIndexRequest();
  request.getRequest().setDatabaseName(DEFAULT_DB_NAME);
  request.getRequest().setTableName(table_name);
  PELOTON_ASSERT(keys.size() > 0);
  // TODO: Set index keys for Multicolumn indexes.
  request.getRequest().setIndexKeys(keys[0]);
  auto response = request.send().wait(client.getWaitScope());
}
}
}
