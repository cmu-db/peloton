#include "backend/bridge/ddl/raw_column_info.h"

namespace peloton {
namespace bridge {

catalog::Column raw_column_info::CreateColumn() const{

  std::vector<catalog::Constraint> constraints;

  catalog::Column column(column_type, column_length,
                         column_name, is_inlined);

  for (auto raw_constraint : raw_constraints) {
    auto constraint = raw_constraint.CreateConstraint();
    column.AddConstraint(constraint);
  }

  return column;
}

}  // namespace bridge
}  // namespace peloton
