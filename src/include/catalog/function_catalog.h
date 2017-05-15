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
// 0: function_oid (pkey)
// 1: function_name (skey)
// 2: owner_oid
// 3: lang_oid
// 4: cost
// 5: rows
// 6: variadic_oid
// 7: isagg_column
// 8: iswindow_column
// 9: secdef_column
// 10: leakproof_column
// 11: isstrict_column
// 12: retset_column
// 13: volatile_column
// 14: num_params
// 15: num_default_params
// 16: rettype_oid
// 17: arg_types
// 18: all_arg_types
// 19: arg_modes
// 20: arg_names
// 21: arg_defaults
// 22: src
// 23: bin
// 24: config
// 25: aclitem

//
// Indexes: (index offset: indexed columns)
// 0: function_oid (unique & primary key)
// 1: function_name (duplicate allowed & secondary key)
//
// Reference for column definitions : https://www.postgresql.org/docs/9.3/static/catalog-pg-proc.html 
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

// information about functions (for FunctionExpression)
struct UDFFunctionData {
  // name of the function
  std::string func_name_;
  // type of input arguments
  std::vector<type::Type::TypeId> argument_types_;
  // names of input parameters
  std::vector<std::string> argument_names_;
  // language name
  PLType language_id_;  
  // funtion's return type
  type::Type::TypeId return_type_;
  // UDF function query string
  std::string func_string_;
  // Indicates whether the function is present in the UDF catalog
  bool func_is_present_;
};

class FunctionCatalog : public AbstractCatalog {
 public:
  ~FunctionCatalog();

  // Global Singleton
  static FunctionCatalog *GetInstance(
      storage::Database *pg_catalog = nullptr,
      type::AbstractPool *pool = nullptr,
      concurrency::Transaction *txn = nullptr);

 inline oid_t GetNextOid() { return oid_++ | FUNCTION_OID_MASK; }
  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  //need to add protransoform, proargtypes, proallargtypes, proargmodes,
  //proargnames, prosrc, proconfig

 ResultType InsertFunction(const std::string &proname,
                         oid_t pronamespace,oid_t proowner, 
                          oid_t prolang, float procost,
                         float prorows,oid_t provariadic,bool proisagg,
                         bool proiswindow,bool prosecdef,bool proleakproof,
                         bool proisstrict,bool proretset,char provolatile,
                          int pronargs,int pronargdefaults, oid_t prorettype,
                          std::vector<type::Type::TypeId> proargtypes, 
                         std::vector<type::Type::TypeId> proallargtypes,
                         std::vector<int> proargmodes, 
                          std::vector<std::string> proargnames, 
                         std::vector<type::Type::TypeId> proargdefaults, 
                          std::vector<std::string> prosrc_bin, 
                         std::vector<std::string> proconfig, std::vector<int> aclitem,
                          type::AbstractPool *pool,
                          concurrency::Transaction *txn);

  bool DeleteFunction(oid_t oid, const std::string &name,
                     concurrency::Transaction *txn);

  UDFFunctionData GetFunction(const std::string &name,concurrency::Transaction *txn );

  oid_t GetFunctionOid(const std::string &name, concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  // Will add later

 private:
  FunctionCatalog(storage::Database *pg_catalog, type::AbstractPool *pool, concurrency::Transaction *txn);

  std::unique_ptr<catalog::Schema> InitializeSchema();

 
};

}  // End catalog namespace
}  // End peloton namespace
