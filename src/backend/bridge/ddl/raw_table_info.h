#pragma once

#include "backend/bridge/ddl/raw_column_info.h"

namespace peloton {
namespace bridge {

class raw_table_info {

public:

  raw_table_info(oid_t table_oid,
                 std::string table_name,
                 std::vector<raw_column_info> raw_columns)
                 : table_oid(table_oid),
                   table_name(table_name),
                   raw_columns(raw_columns) {}

  bool CreateTable(void) const;
 
private:

  oid_t table_oid;
  std::string table_name;
  std::vector<raw_column_info> raw_columns;
};

}  // namespace bridge
}  // namespace peloton
