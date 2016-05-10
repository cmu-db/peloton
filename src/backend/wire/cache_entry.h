#pragma once

#include <string>
#include <sqlite3.h>
#include <vector>

struct CacheEntry {
  std::string stmt_name;
  sqlite3_stmt *sql_stmt;
  std::string query_string;
  std::string query_type;
  std::vector<int32_t> param_types;
};
