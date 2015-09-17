#pragma once

#include "backend/catalog/constraint.h"

namespace peloton {
namespace bridge {

class raw_constraint_info {

public:

  raw_constraint_info(ConstraintType constraint_type,
                      std::string constraint_name,
                      Node *expr = nullptr)
                      : constraint_type(constraint_type),
                        constraint_name(constraint_name),
                        expr(expr) {}

  catalog::Constraint CreateConstraint(void) const;

  void SetDefaultExpr(Node* _expr){
    expr = _expr;
  }

private:

  ConstraintType constraint_type;

  std::string constraint_name;

  // Cooked(transformed) constraint expression
  Node *expr;
};

}  // namespace bridge
}  // namespace peloton
