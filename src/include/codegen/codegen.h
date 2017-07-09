//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// codegen.h
//
// Identification: src/include/codegen/codegen.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "codegen/code_context.h"
#include "codegen/function_builder.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// The main wrapper around LLVM's IR Builder to generate IR
//===----------------------------------------------------------------------===//
class CodeGen {
 public:
  // Constructor
  CodeGen(CodeContext &code_context);

  // Destructor
  ~CodeGen();

  // We forward the -> operator to LLVM's IRBuilder
  llvm::IRBuilder<> *operator->() { return &code_context_.GetBuilder(); }

  // Type wrappers
  llvm::Type *BoolType() const { return code_context_.bool_type_; }
  llvm::Type *Int8Type() const { return code_context_.int8_type_; }
  llvm::Type *ByteType() const { return Int8Type(); }
  llvm::Type *Int16Type() const { return code_context_.int16_type_; }
  llvm::Type *Int32Type() const { return code_context_.int32_type_; }
  llvm::Type *Int64Type() const { return code_context_.int64_type_; }
  llvm::Type *DoubleType() const { return code_context_.double_type_; }
  llvm::Type *VoidType() const { return code_context_.void_type_; }
  llvm::PointerType *CharPtrType() const {
    return code_context_.char_ptr_type_;
  }
  llvm::Type *VectorType(llvm::Type *type, uint32_t num_elements) const {
    return llvm::ArrayType::get(type, num_elements);
  }

  // Constant wrappers for bool, int8, int16, int32, int64, strings and null
  llvm::Constant *ConstBool(bool val) const {
    return llvm::ConstantInt::get(BoolType(), val, true);
  }
  llvm::Constant *Const8(int8_t val) const {
    return llvm::ConstantInt::get(Int8Type(), val, true);
  }
  llvm::Constant *Const16(int16_t val) const {
    return llvm::ConstantInt::get(Int16Type(), val, true);
  }
  llvm::Constant *Const32(int32_t val) const {
    return llvm::ConstantInt::get(Int32Type(), val, true);
  }
  llvm::Constant *Const64(int64_t val) const {
    return llvm::ConstantInt::get(Int64Type(), val, true);
  }
  llvm::Constant *ConstDouble(double val) const {
    return llvm::ConstantFP::get(DoubleType(), val);
  }
  llvm::Constant *ConstString(const std::string s) const;
  llvm::Constant *Null(llvm::Type *type) const {
    return llvm::Constant::getNullValue(type);
  }
  llvm::Constant *NullPtr(llvm::PointerType *type) const {
    return llvm::ConstantPointerNull::get(type);
  }
  // Wrapper for pointer for constant string
  llvm::Value *ConstStringPtr(const std::string s) const;

  // Generate a call to the function with the provided name and arguments
  llvm::Value *CallFunc(llvm::Value *fn,
                        const std::vector<llvm::Value *> &args) const;

  //===--------------------------------------------------------------------===//
  // Call C/C++ standard library functions
  //===--------------------------------------------------------------------===//
  llvm::Value *CallMalloc(uint32_t size, uint32_t alignment = 0);
  llvm::Value *CallFree(llvm::Value *ptr);
  llvm::Value *CallPrintf(const std::string &format,
                          const std::vector<llvm::Value *> &args);

  //===--------------------------------------------------------------------===//
  // Arithmetic with overflow logic
  //===--------------------------------------------------------------------===//

  llvm::Value *CallAddWithOverflow(llvm::Value *left, llvm::Value *right,
                                   llvm::Value *&overflow_bit) const;
  llvm::Value *CallSubWithOverflow(llvm::Value *left, llvm::Value *right,
                                   llvm::Value *&overflow_bit) const;
  llvm::Value *CallMulWithOverflow(llvm::Value *left, llvm::Value *right,
                                   llvm::Value *&overflow_bit) const;
  void ThrowIfOverflow(llvm::Value *overflow) const;
  void ThrowIfDivideByZero(llvm::Value *divide_by_zero) const;

  //===--------------------------------------------------------------------===//
  // Function lookup and registration
  //===--------------------------------------------------------------------===//

  llvm::Function *LookupFunction(const std::string &fn_name) const;
  llvm::Function *RegisterFunction(const std::string &fn_name,
                                   llvm::FunctionType *fn_type);

  llvm::Type *LookupTypeByName(const std::string &name) const;

  // Get the runtime state function argument
  llvm::Value *GetState() const { return &*GetFunction()->arg_begin(); }

  // Get the LLVM context
  llvm::LLVMContext &GetContext() const { return code_context_.GetContext(); }

  // Return the size of the given type in bytes (returns 1 when size < 1 byte)
  uint64_t SizeOf(llvm::Type *type) const;

  // Get the context where all the code we generate resides
  CodeContext &GetCodeContext() const { return code_context_; }

 private:
  friend class Hash;
  friend class Value;
  friend class OAHashTable;

  // Get the current function we're generating code into
  llvm::Function *GetFunction() const {
    return code_context_.GetCurrentFunction()->GetFunction();
  }

  // Get the LLVM module
  llvm::Module &GetModule() const { return code_context_.GetModule(); }

 private:
  // Get the LLVM IR Builder (also accessible through the -> operator overload)
  llvm::IRBuilder<> &GetBuilder() const { return code_context_.GetBuilder(); }

 private:
  // The context/module where all the code this class produces goes
  CodeContext &code_context_;

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(CodeGen);
};

}  // namespace codegen
}  // namespace peloton
