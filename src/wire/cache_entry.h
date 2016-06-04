//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cache_entry.h
//
// Identification: src/wire/cache_entry.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <sqlite3.h>
#include <vector>

struct CacheEntry {
  std::string stmt_name; // logical name of statement
  sqlite3_stmt *sql_stmt;  // pointer to statement allocated by sqlite
  std::string query_string; // query string
  std::string query_type; // first token in query
  std::vector<int32_t> param_types; // format codes of the parameters
};
