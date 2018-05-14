//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_job.cpp
//
// Identification: src/brain/index_selection_job.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection_util.h"
#include "brain/index_selection_job.h"
#include "brain/index_selection.h"
#include "catalog/query_history_catalog.h"
#include "catalog/system_catalogs.h"
#include "optimizer/stats/stats_storage.h"

namespace peloton {
namespace brain {

#define BRAIN_SUGGESTED_INDEX_MAGIC_STR "brain_suggested_index"

void IndexSelectionJob::OnJobInvocation(BrainEnvironment *env) {
  LOG_INFO("Started Index Suggestion Task");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Analyze stats for all the tables.
  // TODO: AnalyzeStatsForAllTables crashes sometimes.
  optimizer::StatsStorage *stats_storage =
      optimizer::StatsStorage::GetInstance();
  ResultType stats_result = stats_storage->AnalyzeStatsForAllTables(txn);
  if (stats_result != ResultType::SUCCESS) {
    LOG_ERROR(
        "Cannot generate stats for table columns. Not performing index "
        "suggestion...");
    txn_manager.AbortTransaction(txn);
    return;
  }

  // Query the catalog for new SQL queries.
  // New SQL queries are the queries that were added to the system
  // after the last_timestamp_
  auto &query_catalog = catalog::QueryHistoryCatalog::GetInstance(txn);
  auto query_history =
      query_catalog.GetQueryStringsAfterTimestamp(last_timestamp_, txn);
  if (query_history->size() > num_queries_threshold_) {
    LOG_INFO("Tuning threshold has crossed. Time to tune the DB!");

    // Run the index selection.
    std::vector<std::string> queries;
    for (auto query_pair : *query_history) {
      queries.push_back(query_pair.second);
    }

    // TODO: Handle multiple databases
    brain::Workload workload(queries, DEFAULT_DB_NAME, txn);
    LOG_INFO("Knob: Num Indexes: %zu",
             env->GetIndexSelectionKnobs().num_indexes_);
    LOG_INFO("Knob: Naive: %zu",
             env->GetIndexSelectionKnobs().naive_enumeration_threshold_);
    LOG_INFO("Knob: Num Iterations: %zu",
             env->GetIndexSelectionKnobs().num_iterations_);
    brain::IndexSelection is = {workload, env->GetIndexSelectionKnobs(), txn};
    brain::IndexConfiguration best_config;
    is.GetBestIndexes(best_config);

    if (best_config.IsEmpty()) {
      LOG_INFO("Best config is empty. No new indexes this time...");
    }

    // Get the index objects from database.
    auto database_object = catalog::Catalog::GetInstance()->GetDatabaseObject(
        DEFAULT_DB_NAME, txn);
    auto pg_index = catalog::Catalog::GetInstance()
                        ->GetSystemCatalogs(database_object->GetDatabaseOid())
                        ->GetIndexCatalog();
    auto cur_indexes = pg_index->GetIndexObjects(txn);
    auto drop_indexes = GetIndexesToDrop(cur_indexes, best_config);

    // Drop useless indexes.
    for (auto index : drop_indexes) {
      LOG_DEBUG("Dropping Index: %s", index_name.c_str());
      DropIndexRPC(database_object->GetDatabaseOid(), index.get());
    }

    // Create new indexes.
    for (auto index : best_config.GetIndexes()) {
      CreateIndexRPC(index.get());
    }

    last_timestamp_ = GetLatestQueryTimestamp(query_history.get());
  } else {
    LOG_INFO("Index Suggestion - not performing this time");
  }
  txn_manager.CommitTransaction(txn);
}

std::vector<std::shared_ptr<catalog::IndexCatalogObject>>
IndexSelectionJob::GetIndexesToDrop(
    std::unordered_map<oid_t, std::shared_ptr<catalog::IndexCatalogObject>> &index_objects,
    brain::IndexConfiguration best_config) {
  std::vector<std::shared_ptr<catalog::IndexCatalogObject>> ret_indexes;
  // Get the existing indexes and drop them.
  for (auto index : index_objects) {
    auto index_name = index.second->GetIndexName();
    // TODO [vamshi]: REMOVE THIS IN THE FINAL CODE
    // This is a hack for now. Add a boolean to the index catalog to
    // find out if an index is a brain suggested index/user created index.
    if (index_name.find(BRAIN_SUGGESTED_INDEX_MAGIC_STR) != std::string::npos) {
      bool found = false;
      for (auto installed_index : best_config.GetIndexes()) {
        if ((index.second.get()->GetTableOid() ==
             installed_index.get()->table_oid) &&
            (index.second.get()->GetKeyAttrs() ==
             installed_index.get()->column_oids)) {
          found = true;
        }
      }
      // Drop only indexes which are not suggested this time.
      if (!found) {
        ret_indexes.push_back(index.second);
      }
    }
  }
  return ret_indexes;
}

void IndexSelectionJob::CreateIndexRPC(brain::HypotheticalIndexObject *index) {
  // TODO: Remove hardcoded database name and server end point.
  capnp::EzRpcClient client("localhost:15445");
  PelotonService::Client peloton_service = client.getMain<PelotonService>();

  // Create the index name: concat - db_id, table_id, col_ids
  std::stringstream sstream;
  sstream << BRAIN_SUGGESTED_INDEX_MAGIC_STR << "_" << index->db_oid << "_"
          << index->table_oid << "_";
  std::vector<oid_t> col_oid_vector;
  for (auto col : index->column_oids) {
    col_oid_vector.push_back(col);
    sstream << col << "_";
  }
  auto index_name = sstream.str();

  auto request = peloton_service.createIndexRequest();
  request.getRequest().setDatabaseOid(index->db_oid);
  request.getRequest().setTableOid(index->table_oid);
  request.getRequest().setIndexName(index_name);
  request.getRequest().setUniqueKeys(false);

  auto col_list =
      request.getRequest().initKeyAttrOids(index->column_oids.size());
  for (auto i = 0UL; i < index->column_oids.size(); i++) {
    col_list.set(i, index->column_oids[i]);
  }

  PELOTON_ASSERT(index->column_oids.size() > 0);
  auto response = request.send().wait(client.getWaitScope());
}

void IndexSelectionJob::DropIndexRPC(oid_t database_oid,
                                     catalog::IndexCatalogObject *index) {
  // TODO: Remove hardcoded database name and server end point.
  capnp::EzRpcClient client("localhost:15445");
  PelotonService::Client peloton_service = client.getMain<PelotonService>();

  auto request = peloton_service.dropIndexRequest();
  request.getRequest().setDatabaseOid(database_oid);
  request.getRequest().setIndexOid(index->GetIndexOid());

  auto response = request.send().wait(client.getWaitScope());
}

uint64_t IndexSelectionJob::GetLatestQueryTimestamp(
    std::vector<std::pair<uint64_t, std::string>> *queries) {
  uint64_t latest_time = 0;
  for (auto query : *queries) {
    if (query.first > latest_time) {
      latest_time = query.first;
    }
  }
  return latest_time;
}
}
}
