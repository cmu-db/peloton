//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// system_catalog.h
//
// Identification: src/include/catalog/system_catalog.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>

#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/trigger_catalog.h"
#include "catalog/table_metrics_catalog.h"
#include "catalog/index_metrics_catalog.h"
#include "catalog/query_metrics_catalog.h"

namespace peloton {

namespace storage {
class Database;
}  // namespace storage

namespace catalog {

class DatabaseCatalog;
class TableCatalog;
class IndexCatalog;
class ColumnCatalog;

class SystemCatalogs {
 public:
  SystemCatalogs() = delete;

  SystemCatalogs(storage::Database *database, type::AbstractPool *pool,
                 concurrency::TransactionContext *txn);

  ~SystemCatalogs();

  void Bootstrap(const std::string &database_name,
                 concurrency::TransactionContext *txn);

  ColumnCatalog *GetColumnCatalog() { return pg_attribute; }
  TableCatalog *GetTableCatalog() { return pg_table; }
  IndexCatalog *GetIndexCatalog() { return pg_index; }
  TriggerCatalog *GetTriggerCatalog() { return pg_trigger; }
  TableMetricsCatalog *GetTableMetricsCatalog() { return pg_table_metrics; }
  IndexMetricsCatalog *GetIndexMetricsCatalog() { return pg_index_metrics; }
  QueryMetricsCatalog *GetQueryMetricsCatalog() { return pg_query_metrics; }

 private:
  ColumnCatalog *pg_attribute;
  TableCatalog *pg_table;
  IndexCatalog *pg_index;

  TriggerCatalog *pg_trigger;
  // ProcCatalog *pg_proc;
  TableMetricsCatalog *pg_table_metrics;
  IndexMetricsCatalog *pg_index_metrics;
  QueryMetricsCatalog *pg_query_metrics;
};

}  // namespace catalog
}  // namespace peloton
