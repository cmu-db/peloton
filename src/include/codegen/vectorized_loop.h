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

#include "include/codegen/util/loop.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A convenience class for generating vectorized loops. This class behaves like
// a regular loop, except callers are responsible for generating inner loops
// based on the current vector range - the current range is acquired through a
// call to GetCurrentRange(...). This class generates a _single_ vectorized loop
// where the last loop iteration _may_ contain a non-full vector. Since we don't
// generated a secondary peeled loop, this reduces the size of generated code
// since we don't duplicate loop body code.
//===----------------------------------------------------------------------===//
class VectorizedLoop {
 public:
  struct Range {
    llvm::Value *start;
    llvm::Value *end;
  };

  // Constructor
  VectorizedLoop(CodeGen &codegen, llvm::Value *num_elements,
                 uint32_t vector_size,
                 const std::vector<util::Loop::LoopVariable> &loop_vars);

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
  util::Loop InitLoop(CodeGen &codegen, llvm::Value *num_elements,
                const std::vector<util::Loop::LoopVariable> loop_vars);

 private:
  // The number of elements to iterate over
  llvm::Value *num_elements_;

  // The loop and whether it has been ended. We track completion to be on the
  // safe side, throwing an assertion exception if not
  util::Loop loop_;
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