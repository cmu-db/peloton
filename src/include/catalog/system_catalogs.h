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
#include "catalog/index_metrics_catalog.h"
#include "catalog/query_metrics_catalog.h"
#include "catalog/schema_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/table_metrics_catalog.h"
#include "catalog/trigger_catalog.h"

namespace peloton {

namespace storage {
class Database;
}  // namespace storage

namespace catalog {
class DatabaseCatalog;
class SchemaCatalog;
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

  //===--------------------------------------------------------------------===//
  // GET FUNCTIONS
  // get catalog tables with name
  //===--------------------------------------------------------------------===//
  ColumnCatalog *GetColumnCatalog() {
    if (!pg_attribute_) {
      throw CatalogException("Column catalog has not been initialized");
    }
    return pg_attribute_;
  }

  SchemaCatalog *GetSchemaCatalog() {
    if (!pg_namespace_) {
      throw CatalogException("schema catalog has not been initialized");
    }
    return pg_namespace_;
  }

  TableCatalog *GetTableCatalog() {
    if (!pg_table_) {
      throw CatalogException("Table catalog has not been initialized");
    }
    return pg_table_;
  }

  IndexCatalog *GetIndexCatalog() {
    if (!pg_index_) {
      throw CatalogException("Index catalog has not been initialized");
    }
    return pg_index_;
  }

  TriggerCatalog *GetTriggerCatalog() {
    if (!pg_trigger_) {
      throw CatalogException("Trigger catalog has not been initialized");
    }
    return pg_trigger_;
  }

  TableMetricsCatalog *GetTableMetricsCatalog() {
    if (!pg_table_metrics_) {
      throw CatalogException("Table metrics catalog has not been initialized");
    }
    return pg_table_metrics_;
  }

  IndexMetricsCatalog *GetIndexMetricsCatalog() {
    if (!pg_index_metrics_) {
      throw CatalogException("Index metrics catalog has not been initialized");
    }
    return pg_index_metrics_;
  }

  QueryMetricsCatalog *GetQueryMetricsCatalog() {
    if (!pg_query_metrics_) {
      throw CatalogException("Query metrics catalog has not been initialized");
    }
    return pg_query_metrics_;
  }

 private:
  ColumnCatalog *pg_attribute_;
  SchemaCatalog *pg_namespace_;
  TableCatalog *pg_table_;
  IndexCatalog *pg_index_;

  TriggerCatalog *pg_trigger_;
  // ProcCatalog *pg_proc;
  TableMetricsCatalog *pg_table_metrics_;
  IndexMetricsCatalog *pg_index_metrics_;
  QueryMetricsCatalog *pg_query_metrics_;
};

}  // namespace catalog
}  // namespace peloton
