//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_catalog.h
//
// Identification: src/include/catalog/function_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_query
//
// Schema: (column offset: column_name)
// 0: name (pkey)
// 1: database_oid (pkey)
// 2: num_params
// 3: param_types
// 4: param_formats
// 5: param_values
// 6: reads
// 7: updates
// 8: deletes
// 9: inserts
// 10: latency
// 11: cpu_time
// 12: time_stamp
//
// Indexes: (index offset: indexed columns)
// 0: name & database_oid (unique & primary key)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

class FunctionCatalog : public AbstractCatalog {
 public:
  ~FunctionCatalog();

  // Global Singleton
  static FunctionCatalog *GetInstance(
      storage::Database *pg_catalog = nullptr,
      type::AbstractPool *pool = nullptr);

 inline oid_t GetNextOid() { return oid_++ | FUNCTION_OID_MASK; }
  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  //need to add protransoform, proargtypes, proallargtypes, proargmodes,
  //proargnames, prosrc, proconfig
  bool InsertFunction(oid_t oid,const std::string &proname,
                          oid_t pronamespace, oid_t proowner, 
                          oid_t prolang, float procost,
                          float prorows, oid_t provariadic, bool proisagg,
                          bool proiswindow, bool prosecdef, bool proleakproof,
                          bool proisstrict, bool proretset, char provolatile,
                          int pronargs, int pronargdefaults, oid prorettype,
                          std::vector<type::Type::TypeId> proargtypes, 
                          std::vector<tpye::Type::TypeId> proallargtypes,
                          std::vector<int> proargmodes, 
                          std::vector<std::string> proargnames, 
                          std::vector<type::Type::TypeId> proargdefaults, 
                          std::vector<string> prosrc_bin, 
                          std::vector<std::string> proconfig, std::vector<int> aclitem,
                          type::AbstractPool *pool,
                          concurrency::Transaction *txn);

  bool DeleteFunction(oid_t oid, const std::string &name,
                     concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  stats::QueryMetric::QueryParamBuf GetParamTypes(
      const std::string &name, oid_t database_oid,
      concurrency::Transaction *txn);
  int64_t GetNumParams(const std::string &name, oid_t database_oid,
                       concurrency::Transaction *txn);
  // TODO: add more if needed

  enum ColumnId {
    NAME = 0,
    DATABASE_OID = 1,
    NUM_PARAMS = 2,
    PARAM_TYPES = 3,
    PARAM_FORMATS = 4,
    PARAM_VALUES = 5,
    READS = 6,
    UPDATES = 7,
    DELETES = 8,
    INSERTS = 9,
    LATENCY = 10,
    CPU_TIME = 11,
    TIME_STAMP = 12,
    // Add new columns here in creation order
  };

 private:
  QueryMetricsCatalog(storage::Database *pg_catalog, type::AbstractPool *pool);

  std::unique_ptr<catalog::Schema> InitializeSchema();

  enum IndexId {
    PRIMARY_KEY = 0,
    // Add new indexes here in creation order
  };
};

}  // End catalog namespace
}  // End peloton namespace
