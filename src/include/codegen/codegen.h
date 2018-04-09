//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// codegen.h
//
// Identification: src/include/codegen/codegen.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "codegen/code_context.h"
#include "codegen/type/type.h"

namespace peloton {
namespace codegen {

// Forward declare
class CodeGen;

/**
 * A CppProxyMember defines an access proxy to a member defined in a C++ class.
 * Users can use this class to generate code to load values from and store
 * values into member variables of C++ classes/structs available at runtime.
 * Each member is defined by a slot position in the C++ struct. Slots are
 * zero-based ordinal numbers assigned to fields increasing order of appearance
 * in the struct/class.
 */
class CppProxyMember {
 public:
  explicit CppProxyMember(uint32_t slot) noexcept : slot_(slot) {}

  /**
   * Load this member field from the provided struct pointer.
   *
   * @param codegen The codegen instance
   * @param obj_ptr A pointer to a runtime C++ struct
   * @return The value of the field
   */
  llvm::Value *Load(CodeGen &codegen, llvm::Value *obj_ptr) const;

  /**
   * Store the provided value into this member field of the provided struct.
   *
   * @param codegen The codegen instance
   * @param obj_ptr A pointer to a runtime C++ struct
   * @param val The value of the field
   */
  void Store(CodeGen &codegen, llvm::Value *obj_ptr, llvm::Value *val) const;

 private:
  // The slot position
  uint32_t slot_;
};

/**
 * The main API used to generate code in Peloton. Provides a thin wrapper around
 * LLVM's IR Builder to generate IR.
 */
class CodeGen {
 public:
  /// Constructor and destructor
  explicit CodeGen(CodeContext &code_context);
  ~CodeGen() = default;

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(CodeGen);

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
  llvm::Type *VoidPtrType() const { return code_context_.void_ptr_type_; }
  llvm::PointerType *CharPtrType() const {
    return code_context_.char_ptr_type_;
  }
  llvm::Type *ArrayType(llvm::Type *type, uint32_t num_elements) const;

  /// Functions to return LLVM values for constant boolean, int8, int16, int32,
  // int64, strings, and null values.
  llvm::Constant *ConstBool(bool val) const;
  llvm::Constant *Const8(int8_t val) const;
  llvm::Constant *Const16(int16_t val) const;
  llvm::Constant *Const32(int32_t val) const;
  llvm::Constant *Const64(int64_t val) const;
  llvm::Constant *ConstDouble(double val) const;
  llvm::Value *ConstString(const std::string &str_val,
                           const std::string &name) const;
  llvm::Value *ConstGenericBytes(const void *data, uint32_t length,
                                 const std::string &name) const;
  llvm::Constant *Null(llvm::Type *type) const;
  llvm::Constant *NullPtr(llvm::PointerType *type) const;

  llvm::Value *AllocateVariable(llvm::Type *type, const std::string &name);
  llvm::Value *AllocateBuffer(llvm::Type *element_type, uint32_t num_elems,
                              const std::string &name);

  // /Generate a call to the function with the provided name and arguments
  llvm::Value *CallFunc(llvm::Value *fn,
                        std::initializer_list<llvm::Value *> args);
  llvm::Value *CallFunc(llvm::Value *fn,
                        const std::vector<llvm::Value *> &args);
  template <typename T>
  llvm::Value *Call(const T &proxy, const std::vector<llvm::Value *> &args) {
    return CallFunc(proxy.GetFunction(*this), args);
  }

  template <typename T>
  llvm::Value *Load(const T &loader, llvm::Value *obj_ptr) {
    return loader.Load(*this, obj_ptr);
  }

  template <typename T>
  void Store(const T &storer, llvm::Value *obj_ptr, llvm::Value *val) {
    storer.Store(*this, obj_ptr, val);
  }

  //===--------------------------------------------------------------------===//
  // C/C++ standard library functions
  //===--------------------------------------------------------------------===//
  llvm::Value *Printf(const std::string &format,
                      const std::vector<llvm::Value *> &args);
  llvm::Value *Memcmp(llvm::Value *ptr1, llvm::Value *ptr2,
                      llvm::Value *len);
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
  std::pair<llvm::Function *, CodeContext::FuncPtr> LookupBuiltin(const std::string &name) const;
  llvm::Function *RegisterBuiltin(const std::string &fn_name,
                                  llvm::FunctionType *fn_type, void *func_impl);

  /// Get the runtime state function argument
  llvm::Value *GetState() const;

  /// Return the size of the given type in bytes (returns 1 when size < 1 byte)
  uint64_t SizeOf(llvm::Type *type) const;
  uint64_t ElementOffset(llvm::Type *type, uint32_t element_idx) const;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  llvm::LLVMContext &GetContext() const { return code_context_.GetContext(); }

  CodeContext &GetCodeContext() const { return code_context_; }

  FunctionBuilder *GetCurrentFunction() const {
    return code_context_.GetCurrentFunction();
  }

  //===--------------------------------------------------------------------===//
  // DEBUG OUTPUT
  //===--------------------------------------------------------------------===//

  static std::string Dump(const llvm::Value *value);
  static std::string Dump(llvm::Type *type);

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
};

}  // namespace codegen
}  // namespace peloton
