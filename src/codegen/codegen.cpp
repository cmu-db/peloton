//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// codegen.cpp
//
// Identification: src/codegen/codegen.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen.h"

#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/Intrinsics.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/TypeBuilder.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"

#include "codegen/function_builder.h"

namespace peloton {
namespace codegen {

CodeGen::CodeGen(CodeContext &code_context) : code_context_(code_context) {}

llvm::Type *CodeGen::ArrayType(llvm::Type *type, uint32_t num_elements) const {
  return llvm::ArrayType::get(type, num_elements);
}

/// Constant wrappers for bool, int8, int16, int32, int64, strings and NULL
llvm::Constant *CodeGen::ConstBool(bool val) const {
  if (val) {
    return llvm::ConstantInt::getTrue(GetContext());
  } else {
    return llvm::ConstantInt::getFalse(GetContext());
  }
}

llvm::Constant *CodeGen::Const8(int8_t val) const {
  return llvm::ConstantInt::get(Int8Type(), val, true);
}

llvm::Constant *CodeGen::Const16(int16_t val) const {
  return llvm::ConstantInt::get(Int16Type(), val, true);
}

llvm::Constant *CodeGen::Const32(int32_t val) const {
  return llvm::ConstantInt::get(Int32Type(), val, true);
}

llvm::Constant *CodeGen::Const64(int64_t val) const {
  return llvm::ConstantInt::get(Int64Type(), val, true);
}

llvm::Constant *CodeGen::ConstDouble(double val) const {
  return llvm::ConstantFP::get(DoubleType(), val);
}

llvm::Value *CodeGen::ConstString(const std::string &str_val,
                                  const std::string &name) const {
  // Strings are treated as arrays of bytes
  auto *str = llvm::ConstantDataArray::getString(GetContext(), str_val);
  auto *global_var =
      new llvm::GlobalVariable(GetModule(), str->getType(), true,
                               llvm::GlobalValue::InternalLinkage, str, name);
  return GetBuilder().CreateInBoundsGEP(global_var, {Const32(0), Const32(0)});
}

llvm::Value *CodeGen::ConstGenericBytes(const void *data, uint32_t length,
                                        const std::string &name) const {
  // Create the constant data array that wraps the input data
  llvm::ArrayRef<uint8_t> elements{reinterpret_cast<const uint8_t *>(data),
                                   length};
  auto *arr = llvm::ConstantDataArray::get(GetContext(), elements);

  // Create a global variable for the data
  auto *global_var =
      new llvm::GlobalVariable(GetModule(), arr->getType(), true,
                               llvm::GlobalValue::InternalLinkage, arr, name);

  // Return a pointer to the first element
  return GetBuilder().CreateInBoundsGEP(global_var, {Const32(0), Const32(0)});
}

llvm::Constant *CodeGen::Null(llvm::Type *type) const {
  return llvm::Constant::getNullValue(type);
}

llvm::Constant *CodeGen::NullPtr(llvm::PointerType *type) const {
  return llvm::ConstantPointerNull::get(type);
}

llvm::Value *CodeGen::AllocateVariable(llvm::Type *type,
                                       const std::string &name) {
  // To allocate a variable, a function must be under construction
  PELOTON_ASSERT(code_context_.GetCurrentFunction() != nullptr);

  // All variable allocations go into the current function's "entry" block. By
  // convention, we insert the allocation instruction before the first
  // instruction in the "entry" block. If the "entry" block is empty, it doesn't
  // matter where we insert it.

  auto *entry_block = code_context_.GetCurrentFunction()->GetEntryBlock();
#if LLVM_VERSION_GE(5, 0)
  if (entry_block->empty()) {
    return new llvm::AllocaInst(type, 0, name, entry_block);
  } else {
    return new llvm::AllocaInst(type, 0, name, &entry_block->front());
  }
#else
  if (entry_block->empty()) {
    return new llvm::AllocaInst(type, name, entry_block);
  } else {
    return new llvm::AllocaInst(type, name, &entry_block->front());
  }
#endif
}

llvm::Value *CodeGen::AllocateBuffer(llvm::Type *element_type,
                                     uint32_t num_elems,
                                     const std::string &name) {
  // Allocate the array
  auto *arr_type = ArrayType(element_type, num_elems);
  auto *alloc = AllocateVariable(arr_type, "");

  // The 'alloca' instruction returns a pointer to the allocated type. Since we
  // are allocating an array of 'element_type' (e.g., i32[4]), we get back a
  // double pointer (e.g., a i32**). Therefore, we introduce a GEP into the
  // buffer to strip off the first pointer reference.

  auto *arr = llvm::GetElementPtrInst::CreateInBounds(
      arr_type, alloc, {Const32(0), Const32(0)}, name);
  arr->insertAfter(llvm::cast<llvm::AllocaInst>(alloc));

  return arr;
}

llvm::Value *CodeGen::CallFunc(llvm::Value *fn,
                               std::initializer_list<llvm::Value *> args) {
  return GetBuilder().CreateCall(fn, args);
}

llvm::Value *CodeGen::CallFunc(llvm::Value *fn,
                               const std::vector<llvm::Value *> &args) {
  return GetBuilder().CreateCall(fn, args);
}

llvm::Value *CodeGen::Printf(const std::string &format,
                             const std::vector<llvm::Value *> &args) {
  auto *printf_fn = LookupBuiltin("printf").first;
  if (printf_fn == nullptr) {
#if GCC_AT_LEAST_6
// In newer GCC versions (i.e., GCC 6+), function attributes are part of the
// type system and are attached to the function signature. For example, printf()
// comes with the "noexcept" attribute. Moreover, GCC 6+ will complain when
// attributes attached to a function (e.g., noexcept()) are not used at
// their call-site. Below, we use decltype(printf) to get the C/C++ function
// type of printf(...), but we discard the attributes since we don't need
// them. Hence, on GCC 6+, compilation will fail without adding the
// "-Wignored-attributes" flag. So, we add it here only.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-attributes"
#endif
    printf_fn = RegisterBuiltin(
        "printf", llvm::TypeBuilder<decltype(printf), false>::get(GetContext()),
        reinterpret_cast<void *>(printf));
#if GCC_AT_LEAST_6
#pragma GCC diagnostic pop
#endif
  }

  // Collect all the arguments into a vector
  std::vector<llvm::Value *> printf_args = {ConstString(format, "format")};
  printf_args.insert(printf_args.end(), args.begin(), args.end());

  // Call printf()
  return CallFunc(printf_fn, printf_args);
}

llvm::Value *CodeGen::Memcmp(llvm::Value *ptr1, llvm::Value *ptr2,
                             llvm::Value *len) {
  static constexpr char kMemcmpFnName[] = "memcmp";
  auto *memcmp_fn = LookupBuiltin(kMemcmpFnName).first;
  if (memcmp_fn == nullptr) {
#if GCC_AT_LEAST_6
// In newer GCC versions (i.e., GCC 6+), function attributes are part of the
// type system and are attached to the function signature. For example, memcmp()
// comes with the "throw()" attribute, among many others. Moreover, GCC 6+ will
// complain when attributes attached to a function are not used at their
// call-site. Below, we use decltype(memcmp) to get the C/C++ function type
// of memcmp(...), but we discard the attributes since we don't need them.
// Hence, on GCC 6+, compilation will fail without adding the
// "-Wignored-attributes" flag. So, we add it here only.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-attributes"
#endif
    memcmp_fn = RegisterBuiltin(
        kMemcmpFnName,
        llvm::TypeBuilder<decltype(memcmp), false>::get(GetContext()),
        reinterpret_cast<void *>(memcmp));
#if GCC_AT_LEAST_6
#pragma GCC diagnostic pop
#endif
  }

  // Call memcmp()
  return CallFunc(memcmp_fn, {ptr1, ptr2, len});
}

llvm::Value *CodeGen::Sqrt(llvm::Value *val) {
  llvm::Function *sqrt_func = llvm::Intrinsic::getDeclaration(
      &GetModule(), llvm::Intrinsic::sqrt, val->getType());
  return CallFunc(sqrt_func, {val});
}

llvm::Value *CodeGen::CallAddWithOverflow(llvm::Value *left, llvm::Value *right,
                                          llvm::Value *&overflow_bit) {
  PELOTON_ASSERT(left->getType() == right->getType());

  // Get the intrinsic that does the addition with overflow checking
  llvm::Function *add_func = llvm::Intrinsic::getDeclaration(
      &GetModule(), llvm::Intrinsic::sadd_with_overflow, left->getType());

  // Perform the addition
  llvm::Value *add_result = CallFunc(add_func, {left, right});

  // Pull out the overflow bit from the resulting aggregate/struct
  overflow_bit = GetBuilder().CreateExtractValue(add_result, 1);

  // Pull out the actual result of the addition
  return GetBuilder().CreateExtractValue(add_result, 0);
}

llvm::Value *CodeGen::CallSubWithOverflow(llvm::Value *left, llvm::Value *right,
                                          llvm::Value *&overflow_bit) {
  PELOTON_ASSERT(left->getType() == right->getType());

  // Get the intrinsic that does the addition with overflow checking
  llvm::Function *sub_func = llvm::Intrinsic::getDeclaration(
      &GetModule(), llvm::Intrinsic::ssub_with_overflow, left->getType());

  // Perform the subtraction
  llvm::Value *sub_result = CallFunc(sub_func, {left, right});

  // Pull out the overflow bit from the resulting aggregate/struct
  overflow_bit = GetBuilder().CreateExtractValue(sub_result, 1);

  // Pull out the actual result of the subtraction
  return GetBuilder().CreateExtractValue(sub_result, 0);
}

llvm::Value *CodeGen::CallMulWithOverflow(llvm::Value *left, llvm::Value *right,
                                          llvm::Value *&overflow_bit) {
  PELOTON_ASSERT(left->getType() == right->getType());
  llvm::Function *mul_func = llvm::Intrinsic::getDeclaration(
      &GetModule(), llvm::Intrinsic::smul_with_overflow, left->getType());

  // Perform the multiplication
  llvm::Value *mul_result = CallFunc(mul_func, {left, right});

  // Pull out the overflow bit from the resulting aggregate/struct
  overflow_bit = GetBuilder().CreateExtractValue(mul_result, 1);

  // Pull out the actual result of the subtraction
  return GetBuilder().CreateExtractValue(mul_result, 0);
}

void CodeGen::ThrowIfOverflow(llvm::Value *overflow) const {
  PELOTON_ASSERT(overflow->getType() == BoolType());

  // Get the overflow basic block for the currently generating function
  auto *func = code_context_.GetCurrentFunction();
  auto *overflow_bb = func->GetOverflowBB();

  // Construct a new block that we jump if there *isn't* an overflow
  llvm::BasicBlock *no_overflow_bb =
      llvm::BasicBlock::Create(GetContext(), "cont", func->GetFunction());

  // Create a branch that goes to the overflow BB if an overflow exists
  auto &builder = GetBuilder();
  builder.CreateCondBr(overflow, overflow_bb, no_overflow_bb);

  // Start insertion in the block
  builder.SetInsertPoint(no_overflow_bb);
}

void CodeGen::ThrowIfDivideByZero(llvm::Value *divide_by_zero) const {
  PELOTON_ASSERT(divide_by_zero->getType() == BoolType());

  // Get the divide-by-zero basic block for the currently generating function
  auto *func = code_context_.GetCurrentFunction();
  auto *div0_bb = func->GetDivideByZeroBB();

  // Construct a new block that we jump if there *isn't* a divide-by-zero
  llvm::BasicBlock *no_div0_bb =
      llvm::BasicBlock::Create(GetContext(), "cont", func->GetFunction());

  // Create a branch that goes to the divide-by-zero BB if an error exists
  auto &builder = GetBuilder();
  builder.CreateCondBr(divide_by_zero, div0_bb, no_div0_bb);

  // Start insertion in the block
  builder.SetInsertPoint(no_div0_bb);
}

// Register the given function symbol and the LLVM function type it represents
llvm::Function *CodeGen::RegisterBuiltin(const std::string &fn_name,
                                         llvm::FunctionType *fn_type,
                                         void *func_impl) {
  // Check if this is already registered as a built in, quit if to
  auto *builtin = LookupBuiltin(fn_name).first;
  if (builtin != nullptr) {
    return builtin;
  }

  // TODO: Function attributes here
  // Construct the function
  auto *function = llvm::Function::Create(
      fn_type, llvm::Function::ExternalLinkage, fn_name, &GetModule());

  // Register the function in the context
  code_context_.RegisterBuiltin(function, func_impl);

  // That's it
  return function;
}

llvm::Type *CodeGen::LookupType(const std::string &name) const {
  return GetModule().getTypeByName(name);
}

std::pair<llvm::Function *, CodeContext::FuncPtr> CodeGen::LookupBuiltin(const std::string &name) const {
  return code_context_.LookupBuiltin(name);
};

llvm::Value *CodeGen::GetState() const {
  auto *func_builder = code_context_.GetCurrentFunction();
  PELOTON_ASSERT(func_builder != nullptr);

  // The first argument of the function is always the runtime state
  return func_builder->GetArgumentByPosition(0);
}

// Return the number of bytes needed to store the given type
uint64_t CodeGen::SizeOf(llvm::Type *type) const {
  auto size = code_context_.GetDataLayout().getTypeSizeInBits(type) / 8;
  return size != 0 ? size : 1;
}

std::string CodeGen::Dump(const llvm::Value *value) {
  std::string string;
  llvm::raw_string_ostream llvm_stream(string);
  llvm_stream << *value;
  return llvm_stream.str();
}

std::string CodeGen::Dump(llvm::Type *type) {
  std::string string;
  llvm::raw_string_ostream llvm_stream(string);
  llvm_stream << *type;
  return llvm_stream.str();
}

uint64_t CodeGen::ElementOffset(llvm::Type *type, uint32_t element_idx) const {
  PELOTON_ASSERT(llvm::isa<llvm::StructType>(type));
  auto &data_layout = code_context_.GetDataLayout();

  auto *struct_layout =
      data_layout.getStructLayout(llvm::cast<llvm::StructType>(type));
  return struct_layout->getElementOffset(element_idx);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Proxy Member
///
////////////////////////////////////////////////////////////////////////////////

llvm::Value *CppProxyMember::Load(CodeGen &codegen,
                                  llvm::Value *obj_ptr) const {
  llvm::SmallVector<llvm::Value *, 2> indexes = {codegen.Const32(0),
                                                 codegen.Const32(slot_)};
  // TODO(pmenon): Use CreateStructGEP()
  llvm::Value *addr = codegen->CreateInBoundsGEP(obj_ptr, indexes);
  return codegen->CreateLoad(addr);
}

void CppProxyMember::Store(CodeGen &codegen, llvm::Value *obj_ptr,
                           llvm::Value *val) const {
  llvm::SmallVector<llvm::Value *, 2> indexes = {codegen.Const32(0),
                                                 codegen.Const32(slot_)};
  llvm::Value *addr = codegen->CreateInBoundsGEP(obj_ptr, indexes);
  codegen->CreateStore(val, addr);
}

}  // namespace codegen
}  // namespace peloton
