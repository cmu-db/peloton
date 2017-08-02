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

#define LANGUAGE_CATALOG_NAME "pg_language"

namespace peloton {
namespace catalog {

class LanguageCatalog : public AbstractCatalog {
 public:
  ~LanguageCatalog();

  // Global Singleton
  static LanguageCatalog *GetInstance(
      concurrency::Transaction *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertLanguage(const std::string &lanname,
                      type::AbstractPool *pool, concurrency::Transaction *txn);

  bool DeleteLanguage(const std::string &lanname,
                      concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  oid_t GetLanguageOid(const std::string &lanname,
                  concurrency::Transaction *txn);

  std::string GetLanguageName(oid_t language_oid,
                              concurrency::Transaction *txn);

  enum ColumnId {
    OID = 0,
    LANNAME = 1,
    // Add new columns here in creation order
  };

 private:
  LanguageCatalog(concurrency::Transaction *txn);

  oid_t GetNextOid() { return oid_++ | LANGUAGE_OID_MASK; }

  enum IndexId {
    PRIMARY_KEY = 0,
    SECONDARY_KEY_0 = 1,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
