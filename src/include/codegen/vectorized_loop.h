//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// vectorized_loop.h
//
// Identification: src/include/codegen/vectorized_loop.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/loop.h"

namespace peloton {
namespace codegen {

class VectorizedLoop {
 public:
  struct Range {
    llvm::Value *start;
    llvm::Value *end;
  };

  // Constructor
  VectorizedLoop(CodeGen &codegen, llvm::Value *num_elements,
                 uint32_t vector_size,
                 const std::vector<Loop::LoopVariable> &loop_vars);

  // Destructor
  ~VectorizedLoop();

  // Get the current range of input the loop is processing
  Range GetCurrentRange() const;

  // Get the loop variable at the given index
  llvm::Value *GetLoopVar(uint32_t index) const;

  // Complete the loop
  void LoopEnd(CodeGen &codegen, const std::vector<llvm::Value *> &loop_vars);

  // Collect the final values of all loop variables
  void CollectFinalLoopVariables(std::vector<llvm::Value *> &loop_vals);

 private:
  // Helper function to initialize the loop
  Loop InitLoop(CodeGen &codegen, llvm::Value *num_elements,
                const std::vector<Loop::LoopVariable> loop_vars);

 private:
  // The number of elements to iterate over
  llvm::Value *num_elements_;

  // The loop and whether it has been ended. We track completion to be on the
  // safe side, throwing an assertion exception if not
  Loop loop_;
  bool ended_;

  // The size of vectors we use during iteration
  uint32_t vector_size_;

  // The current range of iteration.
  // The current loop is iterating from [start , end)
  llvm::Value *start_;
  llvm::Value *end_;
};

}  // namespace codegen
}  // namespace peloton