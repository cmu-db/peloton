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

#include <include/brain/index_selection_util.h>
#include "include/brain/index_selection_job.h"
#include "catalog/query_history_catalog.h"
#include "catalog/system_catalogs.h"
#include "brain/index_selection.h"

namespace peloton {
namespace brain {

#define BRAIN_SUGGESTED_INDEX_MAGIC_STR "brain_suggested_index_"

void IndexSelectionJob::OnJobInvocation(BrainEnvironment *env) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  LOG_INFO("Started Index Suggestion Task");

  // Query the catalog for new queries.
  auto query_catalog = &catalog::QueryHistoryCatalog::GetInstance(txn);
  auto query_history =
      query_catalog->GetQueryStringsAfterTimestamp(last_timestamp_, txn);
  if (query_history->size() > num_queries_threshold_) {
    LOG_INFO("Tuning threshold has crossed. Time to tune the DB!");

    // Run the index selection.
    std::vector<std::string> queries;
    for (auto query_pair : *query_history) {
      queries.push_back(query_pair.second);
    }

    // Get the existing indexes and drop them.
    // TODO: Handle multiple databases
    auto database_object = catalog::Catalog::GetInstance()->GetDatabaseObject(
        DEFAULT_DB_NAME, txn);
    auto pg_index = catalog::Catalog::GetInstance()
                        ->GetSystemCatalogs(database_object->GetDatabaseOid())
                        ->GetIndexCatalog();
    auto indexes = pg_index->GetIndexObjects(txn);
    for (auto index : indexes) {
      auto index_name = index.second->GetIndexName();
      // TODO: This is a hack for now. Add a boolean to the index catalog to
      // find out if an index is a brain suggested index/user created index.
      if (index_name.find(BRAIN_SUGGESTED_INDEX_MAGIC_STR) !=
          std::string::npos) {
        LOG_DEBUG("Dropping Index: %s", index_name.c_str());
        DropIndexRPC(database_object->GetDatabaseOid(), index.second.get());
      }
    }

    // TODO: Handle multiple databases
    brain::Workload workload(queries, DEFAULT_DB_NAME);
    brain::IndexSelection is = {workload, env->GetIndexSelectionKnobs()};
    brain::IndexConfiguration best_config;
    is.GetBestIndexes(best_config);

    for (auto index : best_config.GetIndexes()) {
      // Create RPC for index creation on the server side.
      CreateIndexRPC(index.get());
    }

    // Update the last_timestamp to the be the latest query's timestamp in
    // the current workload, so that we fetch the new queries next time.
    // TODO[vamshi]: Make this efficient. Currently assuming that the latest
    // query can be anywhere in the vector. if the latest query is always at the
    // end, then we can avoid scan over all the queries.
    last_timestamp_ = GetLatestQueryTimestamp(query_history.get());
  } else {
    LOG_INFO("Tuning - not this time");
  }
  txn_manager.CommitTransaction(txn);
}

void IndexSelectionJob::CreateIndexRPC(brain::HypotheticalIndexObject *index) {
  // TODO: Remove hardcoded database name and server end point.
  capnp::EzRpcClient client("localhost:15445");
  PelotonService::Client peloton_service = client.getMain<PelotonService>();

  // Create the index name: concat - db_id, table_id, col_ids
  std::stringstream sstream;
  sstream << BRAIN_SUGGESTED_INDEX_MAGIC_STR << ":" << index->db_oid << ":"
          << index->table_oid << ":";
  std::vector<oid_t> col_oid_vector;
  for (auto col : index->column_oids) {
    col_oid_vector.push_back(col);
    sstream << col << ",";
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
