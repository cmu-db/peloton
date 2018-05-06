//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_sync_brain_job.h
//
// Identification: src/include/brain/catalog_sync_brain_job.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include <vector>
#include <string>
#include <pqxx/pqxx>
#include <algorithm>
#include "concurrency/transaction_manager_factory.h"
#include "brain/brain.h"
#include "catalog/catalog.h"
#include "catalog/table_catalog.h"
#include "storage/data_table.h"
#include "catalog/schema.h"
#include "type/value_factory.h"
#include "catalog/query_history_catalog.h"

namespace peloton {
namespace brain {
/**
 * Brain Job that fetches catalog updates from Peloton
 */
class CatalogSyncBrainJob : public BrainJob {
 public:
  inline CatalogSyncBrainJob(BrainEnvironment *env) : BrainJob(env) {}

  // TODO(tianyu): Eventually use Log for replication
  void OnJobInvocation(BrainEnvironment *env) override;

 private:
  static time_t TimeFromString(const char *str);

  // TODO(tianyu): Switch to Sequence when we have them
  // Returns the SQL string for fetching entries from a catalog table
  std::string FetchCatalogQuery(catalog::AbstractCatalog *catalog);

  // Logs the last entry we have seen in the last_history and last_metric
  void UpdateTimestamp(catalog::AbstractCatalog *catalog, pqxx::field field);

  // Fetches all new entries from the catalog
  void SyncCatalog(catalog::AbstractCatalog *catalog, BrainEnvironment *env,
                   concurrency::TransactionManager &manager);

  // turns a pqxx field into a peloton value
  type::Value PqxxFieldToPelotonValue(pqxx::field &f);

  int64_t last_history_timestamp_ = 0, last_metric_timestamp_ = 0;
};
} // namespace brain
} // nanespace peloton
