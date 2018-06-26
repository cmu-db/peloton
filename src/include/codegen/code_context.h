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
#include <unordered_map>

#include "llvm/IR/IRBuilder.h"

#include "common/macros.h"

namespace llvm {
class ExecutionEngine;
class LLVMContext;
class Module;

namespace legacy {
class FunctionPassManager;
}  // namespace legacy
}  // namespace llvm

namespace peloton {
namespace codegen {

class FunctionBuilder;

namespace interpreter {
class BytecodeBuilder;
}  // namespace interpreter

//===----------------------------------------------------------------------===//
// The context where all generated LLVM query code resides. We create a context
// instance for every query we see.  We keep instances of these around in the
// off-chance that we see a query that requires a function we've previously
// JITed. In reality, this is a thin wrapper around an LLVM Module.
//===----------------------------------------------------------------------===//
class CodeContext {
  friend class CodeGen;
  friend class FunctionBuilder;
  friend class interpreter::BytecodeBuilder;

 public:
  using FuncPtr = void *;

  CodeContext();
  ~CodeContext();

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(CodeContext);

  /// Register a function that will be defined in this context
  void RegisterFunction(llvm::Function *func);

  /// Register a function that is defined externally
  void RegisterExternalFunction(llvm::Function *func_decl, FuncPtr func_impl);

  /// Register a built-in C/C++ function
  void RegisterBuiltin(llvm::Function *func_decl, FuncPtr func_impl);

  /// Lookup a builtin function that has been registered in this context
  std::pair<llvm::Function *, FuncPtr> LookupBuiltin(const std::string &name) const;

  /// Return the LLVM function for UDF that has been registered in this context
  llvm::Function *GetUDF() const { return udf_func_ptr_; }

  /// Sets UDF function ptr
  void SetUDF(llvm::Function *func_ptr) { udf_func_ptr_ = func_ptr; }

  /// Verify all the code contained in this context
  void Verify();

  /// Optimize all the code contained in this context
  void Optimize();

  /// Compile all the code contained in this context
  void Compile();

  /// Retrieve the raw function pointer to the provided compiled LLVM function
  FuncPtr GetRawFunctionPointer(llvm::Function *fn) const;

  /// Get the number of bytes that are needed to store this type
  size_t GetTypeSize(llvm::Type *type) const;

  /// Get the number of bits that are needed to store this type
  size_t GetTypeSizeInBits(llvm::Type *type) const;

  /// Get the number of bytes between two elements of this type
  /// This also includes the padding
  size_t GetTypeAllocSize(llvm::Type *type) const;

  /// Get the number of bits between two elements of this type
  /// This also includes the padding
  size_t GetTypeAllocSizeInBits(llvm::Type *type) const;

  /// Get the offset of element <index> inside a struct in byte
  size_t GetStructElementOffset(llvm::StructType *type, size_t index) const;

  /// Dump the contents of all the code in this context
  /// Attention: this function may change the IR!
  void DumpContents() const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// Get the globally unique identifier for this code
  uint64_t GetID() const { return id_; }

  /// Get the context
  llvm::LLVMContext &GetContext() const { return *context_; }

  /// Get the module
  llvm::Module &GetModule() const { return *module_; }

 private:
  // Get the raw IR in text form
  std::string GetIR() const;

  // Get the IR Builder
  llvm::IRBuilder<> &GetBuilder() { return *builder_; }

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
  std::unique_ptr<llvm::IRBuilder<>> builder_;

  // The function that is currently being generated
  FunctionBuilder *func_;

  // The llvm::Function ptr of the outermost function built
  llvm::Function *udf_func_ptr_;

  // The optimization pass manager
  std::unique_ptr<llvm::legacy::FunctionPassManager> pass_manager_;

  // The JIT compilation engine
  std::string err_str_;
  std::unique_ptr<llvm::ExecutionEngine> engine_;

  // Handy types we reuse often enough to cache here
  llvm::Type *bool_type_;
  llvm::Type *int8_type_;
  llvm::Type *int16_type_;
  llvm::Type *int32_type_;
  llvm::Type *int64_type_;
  llvm::Type *float_type_;
  llvm::Type *double_type_;
  llvm::Type *void_type_;
  llvm::Type *void_ptr_type_;
  llvm::PointerType *char_ptr_type_;

  // All C/C++ builtin functions and their implementations
  std::unordered_map<std::string, std::pair<llvm::Function *, FuncPtr>>
      builtins_;

  // The functions needed in this module, and their implementations. If the
  // function has not been compiled yet, the function pointer will be NULL. The
  // function pointers are populated in Compile()
  std::vector<std::pair<llvm::Function *, FuncPtr>> functions_;

  // Shows if the Verify() has been run
  bool is_verified_;
};

}  // namespace codegen
}  // namespace peloton
