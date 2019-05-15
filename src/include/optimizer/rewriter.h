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
  /**
   * Default constructor
   */
  Rewriter();

  /**
   * Resets the internal state of the rewriter
   */
  void Reset();

  DISALLOW_COPY_AND_MOVE(Rewriter);

  /**
   * Gets the OptimizerMetadata used by the rewriter
   * @returns internal OptimizerMetadata
   */
  OptimizerMetadata &GetMetadata() { return metadata_; }

  /**
   * Rewrites an expression by applying applicable rules
   * @param expr AbstractExpression to rewrite
   * @returns rewriteen AbstractExpression
   */
  expression::AbstractExpression* RewriteExpression(const expression::AbstractExpression *expr);

 private:
  /**
   * Creates an AbstractExpression from the Memo used internally
   * @param root_group GroupID of the root group to begin building from
   * @returns AbstractExpression from the stored groups
   */
  expression::AbstractExpression* RebuildExpression(int root_group);

  /**
   * Performs a single rewrite pass on the epxression
   * @param root_group_id GroupID of the group to start rewriting from
   */
  void RewriteLoop(int root_group_id);

  /**
   * Converts AbstractExpression into internal rewriter representation
   * @param expr expression to convert
   * @returns shared pointer to rewriter internal representation
   */
  std::shared_ptr<AbsExprExpression> ConvertToAbsExpr(const expression::AbstractExpression *expr);

  /**
   * Records the original groups (subtrees) of the AbstractExpression.
   * From the recorded information, it is possible to rebuild the expression.
   * @param expr expression whose groups to record
   * @returns GroupExpression representing the root of the expression
   */
  std::shared_ptr<GroupExpression> RecordTreeGroups(const expression::AbstractExpression *expr);

  /**
   * OptimizerMetadata that we leverage
   */
  OptimizerMetadata metadata_;
};

}  // namespace optimizer
}  // namespace peloton
