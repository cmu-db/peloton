#include "backend/bridge/ddl/raw_constraint_info.h"

namespace peloton {
namespace bridge {

catalog::Constraint raw_constraint_info::CreateConstraint(void) const{
  catalog::Constraint constraint(constraint_type,
                                 constraint_name,
                                 expr);
  return constraint;
}

}  // namespace bridge
}  // namespace peloton

