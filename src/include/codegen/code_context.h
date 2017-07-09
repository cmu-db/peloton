//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// code_context.h
//
// Identification: src/include/codegen/code_context.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LegacyPassManager.h>

#include "common/macros.h"

namespace llvm {
class ExecutionEngine;
class LLVMContext;
class Module;
}  // namespace llvm

namespace peloton {
namespace codegen {

class FunctionBuilder;

//===----------------------------------------------------------------------===//
// The context where all generated LLVM query code resides. We create a context
// instance for every query we see.  We keep instances of these around in the
// off-chance that we see a query that requires a function we've previously
// JITed. In reality, this is a thin wrapper around an LLVM Module.
//===----------------------------------------------------------------------===//
class CodeContext {
  friend class CodeGen;
  friend class FunctionBuilder;

 public:
  CodeContext();
  ~CodeContext();

  // Return the pointer to the LLVM function in this module given its name
  llvm::Function *GetFunction(const std::string &fn_name) const;

  // Get a pointer to the JITed function of the given type
  void *GetFunctionPointer(llvm::Function *fn) const;

  // JIT the code contained within
  bool Compile();

  // Dump the contents of all the code in this context
  void DumpContents() const;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Get the identifier for this code
  uint64_t GetID() const { return id_; }

  // Get the context
  llvm::LLVMContext &GetContext() { return *context_; }

  // Get the module
  llvm::Module &GetModule() { return *module_; }

 private:
  // Get the raw IR in text form
  std::string GetIR() const;

  // Get the IR Builder
  llvm::IRBuilder<> &GetBuilder() { return builder_; }

  // Get the data layout
  const llvm::DataLayout &GetDataLayout() const;

  // Set the current function we're building
  void SetCurrentFunction(FunctionBuilder *func) { func_ = func; }

  // Get the current function we're building
  FunctionBuilder *GetCurrentFunction() const { return func_; }

 private:
  // The ID/version of code
  uint64_t id_;

  // The main context
  std::unique_ptr<llvm::LLVMContext> context_;

  // The LLVM module where all the code goes into
  llvm::Module *module_;

  // The LLVM IR Builder
  llvm::IRBuilder<> builder_;

  // The function that is currently being generated
  FunctionBuilder *func_;

  // The optimization pass manager
  llvm::legacy::FunctionPassManager opt_pass_manager_;

  // The engine we use to ultimately JIT the code in this context
  std::string err_str_;
  std::unique_ptr<llvm::ExecutionEngine> jit_engine_;

  // Handy types we reuse often enough to cache here
  llvm::Type *bool_type_;
  llvm::Type *int8_type_;
  llvm::Type *int16_type_;
  llvm::Type *int32_type_;
  llvm::Type *int64_type_;
  llvm::Type *double_type_;
  llvm::Type *void_type_;
  llvm::PointerType *char_ptr_type_;

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(CodeContext);
};

}  // namespace peloton
}  // namespace peloton
