//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rewriter.h
//
// Identification: src/include/optimizer/rewriter.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "expression/abstract_expression.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/absexpr_expression.h"

namespace peloton {
namespace optimizer {

class Rewriter {

 public:
  Rewriter(const Rewriter &) = delete;
  Rewriter &operator=(const Rewriter &) = delete;
  Rewriter(Rewriter &&) = delete;
  Rewriter &operator=(Rewriter &&) = delete;

  Rewriter();

  expression::AbstractExpression* RewriteExpression(const expression::AbstractExpression *expr);
  void Reset();

  OptimizerMetadata<AbsExpr_Container,ExpressionType,AbsExpr_Expression> &GetMetadata() { return metadata_; }

  std::shared_ptr<AbsExpr_Expression> ConvertToAbsExpr(const expression::AbstractExpression *expr);

 private:
  void ExecuteTaskStack(OptimizerTaskStack<AbsExpr_Container,ExpressionType,AbsExpr_Expression> &task_stack);
  void RewriteLoop(int root_group_id);
  std::shared_ptr<GroupExpression<AbsExpr_Container,ExpressionType,AbsExpr_Expression>> ConvertTree(const expression::AbstractExpression *expr);
  OptimizerMetadata<AbsExpr_Container,ExpressionType,AbsExpr_Expression> metadata_;
};

}  // namespace optimizer
}  // namespace peloton
