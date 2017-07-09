//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_builder.h
//
// Identification: src/include/codegen/function_builder.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/code_context.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A handy class to create a new LLVM function, configure the code context and
// "complete" the function when the user wants to.
//===----------------------------------------------------------------------===//
class FunctionBuilder {
 public:
  // Constructor
  FunctionBuilder(
      CodeContext &code_context, std::string name, llvm::Type *ret_type,
      const std::vector<std::pair<std::string, llvm::Type *>> &args);

  // Destructor
  ~FunctionBuilder();

  // Access to function arguments by argument name or index position
  llvm::Value *GetArgumentByName(std::string name);
  llvm::Value *GetArgumentByPosition(uint32_t index);
  size_t GetNumArguments() const { return func_->arg_size(); }

  // Get the basic block where the function throws an overflow exception
  llvm::BasicBlock *GetOverflowBB();

  // Get the basic block where the function throws a divide by zero exception
  llvm::BasicBlock *GetDivideByZeroBB();

  // Finish the current function
  void ReturnAndFinish(llvm::Value *res = nullptr);

  // Return the function we created in the code context
  llvm::Function *GetFunction() const { return func_; }

 private:
  // Has this function finished construction
  bool finished_;

  // The context into which the generated function will go into
  CodeContext &code_context_;

  // The function that was being constructed at the time our new function is
  // created. We keep this here to restore the state of code generation after
  // our function has finished being constructed.
  FunctionBuilder *previous_function_;
  llvm::BasicBlock *previous_insert_point_;

  // The function we create
  llvm::Function *func_;
  // The entry/first BB in the function
  llvm::BasicBlock *entry_bb_;
  // The arithmetic overflow error block
  llvm::BasicBlock *overflow_bb_;
  // The divide-by-zero error block
  llvm::BasicBlock *divide_by_zero_bb_;

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(FunctionBuilder);
};

}  // namespace codegen
}  // namespace peloton