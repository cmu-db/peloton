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

#include "brain/indextune/lspi/lspi_tuner.h"
#include "brain/index_selection_job_lspi.h"
#include "catalog/query_history_catalog.h"
#include "catalog/system_catalogs.h"
#include "optimizer/stats/stats_storage.h"

namespace peloton {
namespace brain {

bool IndexSelectionJobLSPI::enable_ = false;

IndexSelectionJobLSPI::IndexSelectionJobLSPI(BrainEnvironment *env, uint64_t num_queries_threshold)
: BrainJob(env),
last_timestamp_(0),
num_queries_threshold_(num_queries_threshold) {}

void IndexSelectionJobLSPI::OnJobInvocation(UNUSED_ATTRIBUTE BrainEnvironment *env) {
  LOG_INFO("Started Index Suggestion Task");
  if (!enable_) {
    LOG_INFO("Index Suggestion - not performing this time..Yet to be enabled");
    return;
  }

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Analyze stats for all the tables.
  // TODO: AnalyzeStatsForAllTables crashes sometimes.
//  optimizer::StatsStorage *stats_storage =
//      optimizer::StatsStorage::GetInstance();
//  ResultType stats_result = stats_storage->AnalyzeStatsForAllTables(txn);
//  if (stats_result != ResultType::SUCCESS) {
//    LOG_ERROR(
//        "Cannot generate stats for table columns. Not performing index "
//        "suggestion...");
//    txn_manager.AbortTransaction(txn);
//    return;
//  }



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
    std::vector<double> query_latencies;
    for (auto query_pair : *query_history) {
      queries.push_back(query_pair.second);
    }

    if(!tuner_initialized_ && queries.size() > 0) {
      tuner_initialized_ = true;
      std::set<oid_t> ignore_table_oids;
      CompressedIndexConfigUtil::GetIgnoreTables(DEFAULT_DB_NAME,
                                                 ignore_table_oids);
      tuner_ = std::unique_ptr<LSPIIndexTuner>(new LSPIIndexTuner(DEFAULT_DB_NAME,
                                                                  ignore_table_oids,
                                                                  CandidateSelectionType::Simple,
                                                                  3));
    }

    if(tuner_initialized_) {
      auto container = CompressedIndexConfigUtil::ToIndexConfiguration(*tuner_->GetConfigContainer());
      for(auto query: queries) {
        auto query_latency = brain::CompressedIndexConfigUtil::WhatIfIndexCost(query,
                                                                               container,
                                                                               DEFAULT_DB_NAME);
        query_latencies.push_back(query_latency);
        LOG_DEBUG("Query: %s, What-If cost: %.5f", query.c_str(), query_latency);
      }
      // Run the tuner
      std::set<std::shared_ptr<brain::HypotheticalIndexObject>> add_set, drop_set;
      tuner_->Tune(queries, query_latencies, add_set, drop_set);
      for(auto &index: add_set) {
        LOG_DEBUG("Adding Index: %s", index->ToString().c_str());
        CreateIndexRPC(index.get());
      }
      // Skip dropping for now
//      for(auto &drop_index: drop_set) {
//        LOG_DEBUG("Adding Index: %s", index->ToString().c_str());
//        DropIndexRPC(drop_index.get());
//      }
    }
    last_timestamp_ = GetLatestQueryTimestamp(query_history.get());
  } else {
    LOG_INFO("Index Suggestion - not performing this time");
  }
  txn_manager.CommitTransaction(txn);
}

void IndexSelectionJobLSPI::CreateIndexRPC(brain::HypotheticalIndexObject *index) {
  // TODO: Remove hardcoded database name and server end point.
  capnp::EzRpcClient client("localhost:15445");
  PelotonService::Client peloton_service = client.getMain<PelotonService>();

  // Create the index name: concat - db_id, table_id, col_ids
  std::stringstream sstream;
  sstream << brain_suggested_index_prefix_str << "_" << index->db_oid << "_"
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

void IndexSelectionJobLSPI::DropIndexRPC(oid_t database_oid,
                                     catalog::IndexCatalogObject *index) {
  // TODO: Remove hardcoded database name and server end point.
  // TODO: Have to be removed when merged with tli's code.
  capnp::EzRpcClient client("localhost:15445");
  PelotonService::Client peloton_service = client.getMain<PelotonService>();

  auto request = peloton_service.dropIndexRequest();
  request.getRequest().setDatabaseOid(database_oid);
  request.getRequest().setIndexOid(index->GetIndexOid());

  auto response = request.send().wait(client.getWaitScope());
}

uint64_t IndexSelectionJobLSPI::GetLatestQueryTimestamp(
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
