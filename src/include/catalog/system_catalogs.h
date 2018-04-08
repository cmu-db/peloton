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

  ColumnCatalog *GetColumnCatalog() {
    if (!pg_attribute) {
      throw CatalogException("Column catalog has not been initialized");
    }
    return pg_attribute;
  }
  TableCatalog *GetTableCatalog() {
    if (!pg_table) {
      throw CatalogException("Table catalog has not been initialized");
    }
    return pg_table;
  }
  IndexCatalog *GetIndexCatalog() {
    if (!pg_index) {
      throw CatalogException("Index catalog has not been initialized");
    }
    return pg_index;
  }
  TriggerCatalog *GetTriggerCatalog() {
    if (!pg_trigger) {
      throw CatalogException("Trigger catalog has not been initialized");
    }
    return pg_trigger;
  }
  TableMetricsCatalog *GetTableMetricsCatalog() {
    if (!pg_table_metrics) {
      throw CatalogException("Table metrics catalog has not been initialized");
    }
    return pg_table_metrics;
  }
  IndexMetricsCatalog *GetIndexMetricsCatalog() {
    if (!pg_index_metrics) {
      throw CatalogException("Index metrics catalog has not been initialized");
    }
    return pg_index_metrics;
  }
  QueryMetricsCatalog *GetQueryMetricsCatalog() {
    if (!pg_query_metrics) {
      throw CatalogException("Query metrics catalog has not been initialized");
    }
    return pg_query_metrics;
  }

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
