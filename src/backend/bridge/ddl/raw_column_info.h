#pragma once

#include "backend/bridge/ddl/raw_constraint_info.h"
#include "backend/catalog/column.h"

namespace peloton {
namespace bridge {

class raw_column_info {

public:

  raw_column_info(ValueType column_type,
                  oid_t column_length,
                  std::string column_name,
                  bool is_inlined,
                  std::vector<raw_constraint_info> raw_constraints)
                  : column_type(column_type),
                    column_length(column_length),
                    column_name(column_name),
                    is_inlined(is_inlined),
                    raw_constraints(raw_constraints) {}

  catalog::Column CreateColumn(void) const;

  std::string GetColName()const { return column_name; }

private:

  ValueType column_type;

  size_t column_length;

  std::string column_name;

  bool is_inlined;

  // Constraint information
  std::vector<raw_constraint_info> raw_constraints;

};

}  // namespace bridge
}  // namespace peloton
