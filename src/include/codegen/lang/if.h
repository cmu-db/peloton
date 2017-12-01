//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// if.h
//
// Identification: src/include/codegen/lang/if.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

class Value;

namespace lang {

//===----------------------------------------------------------------------===//
// A utility class to help code-generate if-then-else constructs in LLVM IR
//===----------------------------------------------------------------------===//
class If {
 public:
  // Constructor
  If(CodeGen &cg, llvm::Value *if_condition, const std::string name = "");
  If(CodeGen &cg, const codegen::Value &if_condition,
     const std::string name = "");

  // Begin the else block (provided the name _name_)
  void ElseBlock(const std::string &name = "");

  // End the if/else condition
  void EndIf(llvm::BasicBlock *merge_bb = nullptr);

  // Build a PHI node that merges values generated in each branch of this "if".
  // The first argument must have been generated in the "then" branch. The
  // second value must have been generated either in the "else" branch (if one
  // exists) OR must have exists before the "if" check.
  codegen::Value BuildPHI(const codegen::Value &v1, const codegen::Value &v2);
  llvm::Value *BuildPHI(llvm::Value *v1, llvm::Value *v2);

 private:
  // The code generator class
  CodeGen &cg_;
  // The function
  llvm::Function *fn_;
  // The basic block that contains the code contained in the "then" block
  llvm::BasicBlock *then_bb_;
  llvm::BasicBlock *last_bb_in_then_;
  // The basic block that contains the code contained in the "else" block
  llvm::BasicBlock *else_bb_;
  llvm::BasicBlock *last_bb_in_else_;
  // The block at the end that both the "then" and "else" blocks merge to
  llvm::BasicBlock *merge_bb_;
  // The actual branch instruction.  We need this in the case where the caller
  // wants to introduce an "else" condition.  In this case, we need to modify
  // the branch call to fall to the "else" block if the condition is false,
  // rather than to the merging BB as we do in the constructor.
  llvm::BranchInst *branch_;

  // Name of the hash table, that will be used to make the IR labels more meaningful
  const std::string name_;
};

}  // namespace lang
}  // namespace codegen
}  // namespace peloton