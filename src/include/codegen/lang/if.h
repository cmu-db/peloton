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
 * and place the code to be executed if the condition is met inside a pair of
 * braces as follows:
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
 * EndIf() is used to mark the completion/closing of the if-statement. This
 * format resembles conditionals in native C/C++ - the code contained within the
 * brace is only executed if the if-condition is evaluated to true. If the
 * condition is evaluated to false, the braced-code is skipped and execution
 * continues after the call to EndIf(). Similarly, callers can also generate an
 * "else" clause as follows:
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
 * The above examples demonstrate using the simple API. This API allows the
 * caller to generate an if-then-else constructs naturally, as they could in
 * C/C++, without relying on internal details of how the underlying IR works.
 * This is the API most users should use.
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
 * In the above example, if the condition is evaluated to true, the code within
 * the brace is executed first, then continues after the EndIf(). If the
 * condition is evaluated to false, execution jumps to the "else_bb" block
 * provided by the caller. If the else_bb block does not end in a terminator
 * instruction, it will automatically jump to the first instruction after the
 * EndIf(). This is where the advanced component comes in - you had better know
 * what you're doing!
 *
 * Either of the "then" or "else" blocks can be provided using the advanced API.
 * If neither are provided, the behaviour matches the simple API.
 *
 * PHIs:
 * -----
 * If you have an if-statement that generates or assigns values in either
 * branches, and you would like to have them available after the if-statement,
 * you need to generate a PHI instruction that merges them. In our IR, variables
 * can only be assigned once (SSA); hence, if you want a variable to take on a
 * value depending on which branch is taken, the way this is done is to create a
 * new variable **after** the if-statement that "merges" the values from each
 * branch into a single variable. In C, this might look like:
 *
 * @code
 * uint32_t x;
 * if (y < 10) {
 *   x = 4;
 * } else {
 *   x = 5;
 * }
 * // use x ...
 * @endcode
 *
 * Because of SSA, the above code will look like the following in our IR:
 *
 * @code
 * if (y < 10) {
 *   uint32_t x1 = 4;
 * } else {
 *   uint32_t x2 = 5;
 * }
 * uint32_t x = phi_merge(x1, x2);
 * @endcode
 *
 * The above is a gross simplification of what's actually happening, but shows
 * why we need PHI instructions.
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