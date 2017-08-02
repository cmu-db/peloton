//===----------------------------------------------------------------------===//
// pg_proc
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

#define PROC_CATALOG_NAME "pg_proc"

namespace peloton {
namespace catalog {

class ProcCatalog : public AbstractCatalog {
 public:
  ~ProcCatalog();

  // Global Singleton
  static ProcCatalog *GetInstance(
      concurrency::Transaction *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertProc(const std::string &proname,
                  type::TypeId prorettype,
                  const std::vector<type::TypeId> &proargtypes,
                  oid_t prolang,
                  const std::string &prosrc,
                  type::AbstractPool *pool, concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::string GetProSrc(const std::string &proname,
                        const std::vector<type::TypeId> &proargtypes,
                        concurrency::Transaction *txn);

  type::TypeId GetProRetType(const std::string &proname,
                             const std::vector<type::TypeId> &proargtypes,
                             concurrency::Transaction *txn);

  oid_t GetProLang(const std::string &proname,
                   const std::vector<type::TypeId> &proargtypes,
                   concurrency::Transaction *txn);

  enum ColumnId {
    OID = 0,
    PRONAME = 1,
    PROARGTYPES = 2,
    PRORETTYPE = 3,
    PROLANG = 4,
    PROSRC = 5,
    // Add new columns here in creation order
  };

 private:
  ProcCatalog(concurrency::Transaction *txn);

  oid_t GetNextOid() { return oid_++ | PROC_OID_MASK; }

  std::string& TypeArrayToString(const std::vector<type::TypeId> types);

  std::vector<type::TypeId>& StringToTypeArray(const std::string &types);

  enum IndexId {
    PRIMARY_KEY = 0,
    SECONDARY_KEY_0 = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
