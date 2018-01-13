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
#include <vector>

#include "codegen/code_context.h"

namespace peloton {
namespace codegen {

class CodeGen;

/// A proxy to a member of a class. Users must provide both the physical
/// position of the member in the class, and the C++ type of the member.
template <uint32_t Pos, typename T>
struct ProxyMember {
  // Virtual destructor
  virtual ~ProxyMember() = default;
};

/// A proxy to a method in a class. Subclasses must implement GetFunction().
template <typename T>
struct ProxyMethod {
  // Virtual destructor
  virtual ~ProxyMethod() = default;

  // Hand off to the specialized template to define the LLVM function
  llvm::Function *GetFunction(CodeGen &codegen) {
    return static_cast<T *>(this)->GetFunction(codegen);
  }
};

//===----------------------------------------------------------------------===//
// The main wrapper around LLVM's IR Builder to generate IR
//===----------------------------------------------------------------------===//
class CodeGen {
 public:
  /// Constructor and destructor
  explicit CodeGen(CodeContext &code_context);
  ~CodeGen() = default;

  /// We forward the -> operator to LLVM's IRBuilder
  llvm::IRBuilder<> *operator->() { return &GetBuilder(); }

  /// Type wrappers
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
  llvm::Type *ArrayType(llvm::Type *type, uint32_t num_elements) const;

  /// Constant wrappers for bool, int8, int16, int32, int64, strings, and null
  llvm::Constant *ConstBool(bool val) const;
  llvm::Constant *Const8(uint8_t val) const;
  llvm::Constant *Const16(uint16_t val) const;
  llvm::Constant *Const32(uint32_t val) const;
  llvm::Constant *Const64(uint64_t val) const;
  llvm::Constant *ConstDouble(double val) const;
  llvm::Constant *ConstString(const std::string &s) const;
  llvm::Constant *Null(llvm::Type *type) const;
  llvm::Constant *NullPtr(llvm::PointerType *type) const;
  /// Wrapper for pointer for constant string
  llvm::Value *ConstStringPtr(const std::string &s) const;

  llvm::Value *AllocateVariable(llvm::Type *type, const std::string &name);
  llvm::Value *AllocateBuffer(llvm::Type *element_type, uint32_t num_elems,
                              const std::string &name);

  // /Generate a call to the function with the provided name and arguments
  llvm::Value *CallFunc(llvm::Value *fn,
                        std::initializer_list<llvm::Value *> args);
  llvm::Value *CallFunc(llvm::Value *fn,
                        const std::vector<llvm::Value *> &args);
  template <typename T>
  llvm::Value *Call(ProxyMethod<T> &proxy,
                    const std::vector<llvm::Value *> &args) {
    return CallFunc(proxy.GetFunction(*this), args);
  }

  //===--------------------------------------------------------------------===//
  // C/C++ standard library functions
  //===--------------------------------------------------------------------===//
  llvm::Value *CallPrintf(const std::string &format,
                          const std::vector<llvm::Value *> &args);
  llvm::Value *Sqrt(llvm::Value *val);

  //===--------------------------------------------------------------------===//
  // Arithmetic with overflow logic - These methods perform the desired math op,
  // on the provided left and right argument and return the result of the op
  // and set the overflow_but out-parameter. It is up to the caller to decide
  // how to handle an overflow.
  //===--------------------------------------------------------------------===//
  llvm::Value *CallAddWithOverflow(llvm::Value *left, llvm::Value *right,
                                   llvm::Value *&overflow_bit);
  llvm::Value *CallSubWithOverflow(llvm::Value *left, llvm::Value *right,
                                   llvm::Value *&overflow_bit);
  llvm::Value *CallMulWithOverflow(llvm::Value *left, llvm::Value *right,
                                   llvm::Value *&overflow_bit);
  void ThrowIfOverflow(llvm::Value *overflow) const;
  void ThrowIfDivideByZero(llvm::Value *divide_by_zero) const;

  //===--------------------------------------------------------------------===//
  // Function lookup and registration
  //===--------------------------------------------------------------------===//
  llvm::Type *LookupType(const std::string &name) const;
  llvm::Function *LookupBuiltin(const std::string &fn_name) const {
    return code_context_.LookupBuiltin(fn_name);
  }
  llvm::Function *RegisterBuiltin(const std::string &fn_name,
                                  llvm::FunctionType *fn_type, void *func_impl);

  /// Get the runtime state function argument
  llvm::Value *GetState() const;

  /// Return the size of the given type in bytes (returns 1 when size < 1 byte)
  uint64_t SizeOf(llvm::Type *type) const;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  llvm::LLVMContext &GetContext() const { return code_context_.GetContext(); }

  CodeContext &GetCodeContext() const { return code_context_; }

  FunctionBuilder *GetCurrentFunction() const {
    return code_context_.GetCurrentFunction();
  }

 private:
  friend class Hash;
  friend class Value;
  friend class OAHashTable;

  // Get the LLVM module
  llvm::Module &GetModule() const { return code_context_.GetModule(); }

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
