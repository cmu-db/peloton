//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_logger.h
//
// Identification: src/include/brain/query_logger.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <string>

#include "parser/pg_query.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// QueryLogger
//===--------------------------------------------------------------------===//

class QueryLogger {
 public:
  QueryLogger() = default;

  class Fingerprint {
   public:
    /// Constructor
    explicit Fingerprint(const std::string &query);

    /// Destructor
    ~Fingerprint();

    /// Get original string
    const std::string &GetQueryString() { return query_; }
    const std::string &GetFingerprint() { return fingerprint_; }

   private:
    // Accessors
    std::string query_;
    std::string fingerprint_;
    PgQueryFingerprintResult fingerprint_result_;
  };

  /**
   * @brief This function logs the query into query_history_catalog
   *
   * @param the sql string corresponding to the query
   * @param timestamp of the transaction that executed the query
   */
  static void LogQuery(std::string query_string, uint64_t timestamp);
};

}  // namespace brain
}  // namespace peloton
