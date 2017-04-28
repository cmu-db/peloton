//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// loop.h
//
// Identification: src/include/codegen/loop.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A utility class to help generate loops in LLVM IR
//===----------------------------------------------------------------------===//
class Loop {
 public:
  struct LoopVariable {
    std::string name;
    llvm::Value *val;
  };

  // Constructor
  Loop(CodeGen &cg, llvm::Value *start_condition,
       const std::vector<LoopVariable> &loop_vars);

  // Get the loop variable at the given index
  llvm::Value *GetLoopVar(uint32_t id) const;

  // Mark the end of the loop block.  The loop continues at the top if the end
  // condition is true
  void LoopEnd(llvm::Value *end_condition,
               const std::vector<llvm::Value *> &next);

  // Collect the final values of all loop variables
  void CollectFinalLoopVariables(std::vector<llvm::Value *> &loop_vals);

 private:
  // The code generator
  CodeGen &cg_;
  // The function the loop is in
  llvm::Function *fn_;
  // The predecessor basic block of this loop
  llvm::BasicBlock *pre_loop_bb_;
  // The block where the code for the body of the loop goes
  llvm::BasicBlock *loop_bb_;
  // The last BB _inside_ the loop
  llvm::BasicBlock *last_loop_bb_;
  // The block at the bottom where the loop jumps to when the loop condition
  // becomes false
  llvm::BasicBlock *end_bb_;
  // The list of PHI nodes (i.e. loop variables)
  std::vector<llvm::PHINode *> phi_nodes_;
};

}  // namespace codegen
}  // namespace peloton