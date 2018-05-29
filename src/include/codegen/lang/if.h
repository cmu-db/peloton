//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// if.h
//
// Identification: src/include/codegen/lang/if.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

class Value;

namespace lang {

/**
 * @brief A utility class to help generate if-then-else constructs in LLVM IR.
 *
 * Generally, callers construct an instance of this class (usually on the stack)
 * and place the code to be executed if the condition is met inside a brace as
 * follows:
 *
 * @code
 * llvm::Value *cond = left.CompareEq(codegen, right);
 * lang::If cond_is_met(codegen, cond);
 * {
 *   // True branch
 *   codegen.Printf("Condition is true\n");
 * }
 * cond_is_met.EndIf();
 * @endcode
 *
 * EndIf() must be called to complete the generation of the if-statement. The
 * caller can also generate the "false" branch (i.e., else) as follows:
 *
 * @code
 * llvm::Value *cond = left.CompareEq(codegen, right);
 * lang::If cond_is_met(codegen, cond);
 * {
 *   // True branch
 *   codegen.Printf("Condition is true\n");
 * }
 * cond_is_met.ElseBlock();
 * {
 *   // False branch
 *   codegen.Printf("Condition is NOT true\n");
 * }
 * cond_is_met.EndIf();
 * @endcode
 *
 * The above examples demonstrate how to use the simple API. The simple allows
 * the caller to "interactively" generate an if-then-else statement without
 * relying on internal details of how the underlying IR works. This is the
 * API most users should use.
 *
 * An advanced API also exists. The advanced API allows the caller to specify
 * the blocks to jump to in the "true" and "false" branch for the condition.
 * This is helpful in cases where these blocks exist before the generation of
 * the "if" statement. For example:
 *
 * @code
 * llvm::BasicBlock *else_bb = // created before hand
 * llvm::Value *cond = left.CompareEq(codegen, right);
 * lang::If cond_is_met(codegen, cond, nullptr, else_bb);
 * {
 *   // True branch
 *   codegen.Printf("Condition is true\n");
 * }
 * cond_is_met.EndIf();
 * @endcode
 *
 * In the above example, the caller generated the code in the "true" branch
 * immediately; in the false branch, the caller wants to jump to the provided
 * block.
 */
class If {
 public:
  /**
   * This constructor generates an if-then using the provided condition.
   * After construction, the provided CodeGen cursor moves to the first
   * instruction to execute if the provided condition is true.
   *
   * @param codegen The codegen instance
   * @param cond The condition to branch against
   * @param name A name to assign to this "if" statement. This is mostly for
   * debugging.
   */
  If(CodeGen &codegen, llvm::Value *cond, std::string name = "then");
  If(CodeGen &codegen, const codegen::Value &cond, std::string name = "then");

  /**
   * This constructor enables the generation of a full-blown if-then-else using
   * the provided condition.
   *
   * @param codegen The codegen instance
   * @param cond The condition to branch against
   * @param name A name to assign to this "if" statement. This is mostly for
   * debugging.
   * @param then_bb The block to branch to if the condition is true
   * @param else_bb The block to branch to if the condition is false
   */
  If(CodeGen &codegen, llvm::Value *cond, std::string name,
     llvm::BasicBlock *then_bb, llvm::BasicBlock *else_bb);
  If(CodeGen &codegen, const codegen::Value &cond, std::string name,
     llvm::BasicBlock *then_bb, llvm::BasicBlock *else_bb);

  /**
   * Start generation for the "else" branch
   */
  void ElseBlock();

  /**
   * Finish the if-then-else structure
   */
  void EndIf();

  /**
   * Build a PHI node that merges values generated in each branch of this "if".
   *
   * @param v1 A value produced in the "then" (i.e., true) branch of this "if".
   * @param v2 A value that either existed **before** this "if", or was
   * generated in the "else" (i.e., false) branch of this "if"
   * @return A value that finalizes the value that potentially was produced in
   * two different code paths.
   */
  codegen::Value BuildPHI(const codegen::Value &v1, const codegen::Value &v2);
  llvm::Value *BuildPHI(llvm::Value *v1, llvm::Value *v2);

 private:
  /**
   * Common initialization across all constructors.
   *
   * @param cond The condition for the "if"
   * @param then_bb The (optional) block to move in the "true" branch
   * @param else_bb The (optional) block to move in the "false" branch
   */
  void Init(llvm::Value *cond, llvm::BasicBlock *then_bb,
            llvm::BasicBlock *else_bb);

  /**
   * Branch to the provided block if the current block is not terminated
   * (i.e., doesn't end in a "terminator" instruction such as a branch or
   * return instruction etc.)
   *
   * @param block The block to branch to
   */
  void BranchIfNotTerminated(llvm::BasicBlock *block) const;

 private:
  // The code generator class
  CodeGen &codegen_;

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
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline If::If(CodeGen &codegen, llvm::Value *cond, std::string name)
    : If(codegen, cond, std::move(name), nullptr, nullptr) {}

inline If::If(CodeGen &codegen, const codegen::Value &cond, std::string name)
    : If(codegen, cond, std::move(name), nullptr, nullptr) {}

}  // namespace lang
}  // namespace codegen
}  // namespace peloton