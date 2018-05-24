//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// limit_translator.h
//
// Identification: src/include/codegen/operator/limit_translator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/operator/operator_translator.h"

namespace peloton {

namespace planner {
class LimitPlan;
}  // namespace planner

namespace codegen {

/**
 * This is a translator for the LIMIT operator when applied without a sort.
 * There is a separate translator for the optimized, merged SORT+LIMIT operator.
 */
class LimitTranslator : public OperatorTranslator {
 public:
  LimitTranslator(const planner::LimitPlan &plan, CompilationContext &context,
                  Pipeline &pipeline);

  void InitializeQueryState() override;

  // No extra functions needed
  void DefineAuxiliaryFunctions() override {}

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  // No state to teat down
  void TearDownQueryState() override {}

 private:
  /**
   * Given the current count, determine if it falls in the active window between
   * 'offset' and 'limit' tuples.
   *
   * @param codegen The codegen instance
   * @param count The current count of tuples we've seen
   * @return True (as an LLVM value) if in the window. False otherwise.
   */
  llvm::Value *InValidWindow(CodeGen &codegen, llvm::Value *count) const;

 private:
  // We keep an 8-byte count
  QueryState::Id limit_count_id_;
};

}  // namespace codegen
}  // namespace peloton