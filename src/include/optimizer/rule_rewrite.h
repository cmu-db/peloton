//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rule_rewrite.h
//
// Identification: src/include/optimizer/rule_rewrite.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.h"
#include "optimizer/absexpr_expression.h"

#include <memory>

namespace peloton {
namespace optimizer {

class ComparatorElimination: public Rule<AbsExpr_Container,ExpressionType,AbsExpr_Expression> {
 public:
  ComparatorElimination();

  int Promise(GroupExpression<AbsExpr_Container,ExpressionType,AbsExpr_Expression> *group_expr,
              OptimizeContext<AbsExpr_Container,ExpressionType,AbsExpr_Expression> *context) const override;

  bool Check(std::shared_ptr<AbsExpr_Expression> plan,
             OptimizeContext<AbsExpr_Container,ExpressionType,AbsExpr_Expression> *context) const override;

  void Transform(std::shared_ptr<AbsExpr_Expression> input,
                 std::vector<std::shared_ptr<AbsExpr_Expression>> &transformed,
                 OptimizeContext<AbsExpr_Container,ExpressionType,AbsExpr_Expression> *context) const override;
};
}  // namespace optimizer
}  // namespace peloton
