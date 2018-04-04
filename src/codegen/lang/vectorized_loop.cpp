//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// vectorized_loop.cpp
//
// Identification: src/codegen/lang/vectorized_loop.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/lang/vectorized_loop.h"

#include "common/exception.h"

namespace peloton {
namespace codegen {
namespace lang {

VectorizedLoop::VectorizedLoop(CodeGen &codegen, llvm::Value *num_elements,
                               uint32_t vector_size,
                               const std::vector<Loop::LoopVariable> &loop_vars)
    : num_elements_(
          codegen->CreateZExtOrBitCast(num_elements, codegen.Int32Type())),
      loop_(InitLoop(codegen, num_elements_, loop_vars)),
      ended_(false),
      vector_size_(vector_size) {
  start_ = loop_.GetLoopVar(0);
  end_ = codegen->CreateAdd(start_, codegen.Const32(vector_size_));
  end_ = codegen->CreateSelect(codegen->CreateICmpULT(num_elements_, end_),
                               num_elements_, end_);
}

VectorizedLoop::~VectorizedLoop() {
  PELOTON_ASSERT(ended_ && "You didn't call lang::VectorizedLoop::LoopEnd()!");
}

VectorizedLoop::Range VectorizedLoop::GetCurrentRange() const {
  return {start_, end_};
}

llvm::Value *VectorizedLoop::GetLoopVar(uint32_t index) const {
  // Need to add one because we inserted a hidden loop variable to track start
  return loop_.GetLoopVar(index + 1);
}

void VectorizedLoop::LoopEnd(CodeGen &codegen,
                             const std::vector<llvm::Value *> &loop_vars) {
  llvm::Value *next_start =
      codegen->CreateAdd(start_, codegen.Const32(vector_size_));
  std::vector<llvm::Value *> next = {next_start};
  next.insert(next.end(), loop_vars.begin(), loop_vars.end());
  loop_.LoopEnd(codegen->CreateICmpULT(next_start, num_elements_), next);
  ended_ = true;
}

Loop VectorizedLoop::InitLoop(CodeGen &codegen, llvm::Value *num_elements,
                              const std::vector<Loop::LoopVariable> loop_vars) {
  llvm::Value *start = codegen.Const32(0);
  std::vector<Loop::LoopVariable> all_loop_vars = {{"start", start}};
  all_loop_vars.insert(all_loop_vars.end(), loop_vars.begin(), loop_vars.end());

  llvm::Value *loop_cond = codegen->CreateICmpULT(start, num_elements);
  return Loop{codegen, loop_cond, all_loop_vars};
}

void VectorizedLoop::CollectFinalLoopVariables(
    std::vector<llvm::Value *> &loop_vals) {
  loop_.CollectFinalLoopVariables(loop_vals);
  loop_vals.erase(loop_vals.begin());
}

}  // namespace lang
}  // namespace codegen
}  // namespace peloton