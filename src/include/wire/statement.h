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
#include <vector>

#include <sqlite3.h>

struct Statement {
  // logical name of statement
  std::string stmt_name;

  // pointer to statement allocated by sqlite
  sqlite3_stmt *sql_stmt;

  // query string
  std::string query_string;

  // first token in query
  std::string query_type;

  // format codes of the parameters
  std::vector<int32_t> param_types;
};
