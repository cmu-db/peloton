//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_metrics_catalog.h
//
// Identification: src/include/catalog/trigger_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_trigger
//
// Schema: (column offset: column_name)
// 0: trigger_name (pkey)
// 1: database_oid (pkey)
// 2: table_oid
// 3: trigger_type
// 4: fire_condition
// 5: function_name
// 6: function_arguments
// 7: time_stamp
//
// Indexes: (index offset: indexed columns)
// 0: database_oid & table_oid & trigger_type
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "commands/trigger.h"

#define TRIGGER_CATALOG_NAME "pg_trigger"

namespace peloton {
namespace catalog {

class TriggerCatalog : public AbstractCatalog {
 public:
  ~TriggerCatalog();

  // Global Singleton
  static TriggerCatalog *GetInstance(
      concurrency::Transaction *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertTrigger(std::string trigger_name, oid_t database_oid, oid_t table_oid,
                          int16_t trigger_type,
                          std::string fire_condition, //TODO: this actually should be expression
                          std::string function_name,
                          std::string function_arguments,
                          int64_t time_stamp, type::AbstractPool *pool,
                          concurrency::Transaction *txn);

  // // TODO:
  // bool DeleteTrigger(const std::string &trigger_name, oid_t database_oid, oid_t table_oid, 
  //                         concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // get triggers for a specific table; one table may have multiple triggers
  // of the same type
  //===--------------------------------------------------------------------===//
  commands::TriggerList* GetTriggers(
      oid_t database_oid, oid_t table_oid, int16_t trigger_type,
      concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // get all types of triggers for a specific table
  //===--------------------------------------------------------------------===//
  commands::TriggerList* GetTriggers(
      oid_t database_oid, oid_t table_oid, concurrency::Transaction *txn);


  enum ColumnId {
    TRIGGER_NAME = 0,
    DATABASE_OID = 1,
    TABLE_OID = 2,
    TRIGGER_TYPE = 3,
    FIRE_CONDITION = 4,
    FUNCTION_NAME = 5,
    FUNCTION_ARGS = 6,
    TIME_STAMP = 7
    // Add new columns here in creation order
  };

 private:
  TriggerCatalog(concurrency::Transaction *txn);

  enum IndexId {
    SECONDARY_KEY_0 = 0,
    // Add new indexes here in creation order
    DATABASE_TABLE_KEY_1 = 1,
  };

};

}  // End catalog namespace
}  // End peloton namespace
