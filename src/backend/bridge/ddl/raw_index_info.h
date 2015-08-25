#pragma once

#include "backend/bridge/ddl/ddl_index.h"

#include <vector>

namespace peloton {
namespace bridge {

class raw_index_info {

public:

  raw_index_info(oid_t index_oid,
                 std::string index_name,
                 std::string table_name,
                 IndexType method_type,
                 IndexConstraintType constraint_type,
                 bool unique_keys,
                 std::vector<std::string> key_column_names)
                 : index_oid(index_oid),
                   index_name(index_name),
                   table_name(table_name),
                   method_type(method_type),
                   constraint_type(constraint_type),
                   unique_keys(unique_keys),
                   key_column_names(key_column_names) {}

  bool CreateIndex(void) const;

private:

  Oid index_oid;
  std::string index_name;
  std::string table_name;

  IndexType method_type;
  IndexConstraintType constraint_type;

  bool unique_keys;

  std::vector<std::string> key_column_names;
};

}  // namespace bridge
}  // namespace peloton
