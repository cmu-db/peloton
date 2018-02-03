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

#include <string>

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// QueryLogger
//===--------------------------------------------------------------------===//

class QueryLogger {
 public:
  QueryLogger() = default;

  /*
   * @brief This function logs the query into query_history_catalog
   * @param the sql string corresponding to the query
   * @param timestamp of the transaction that executed the query
   */
  static void LogQuery(std::string query_string, uint64_t timestamp);
};

}  // namespace brain
}  // namespace peloton
