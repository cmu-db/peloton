//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// codegen.cpp
//
// Identification: src/codegen/codegen.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen.h"

#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/IR/Intrinsics.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/TypeBuilder.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/Scalar.h"

namespace peloton {
namespace codegen {

CodeGen::CodeGen(CodeContext &code_context) : code_context_(code_context) {}

CodeGen::~CodeGen() {}

llvm::Constant *CodeGen::ConstString(const std::string s) const {
  // Strings are treated as arrays of bytes
  auto *str = llvm::ConstantDataArray::getString(GetContext(), s.c_str());
  return new llvm::GlobalVariable(GetModule(), str->getType(), true,
                                  llvm::GlobalValue::InternalLinkage, str,
                                  "str");
}

llvm::Value *CodeGen::ConstStringPtr(const std::string s) const {
  return GetBuilder().CreateConstInBoundsGEP2_32(NULL, ConstString(s), 0, 0,
                                                 "");
}

llvm::Value *CodeGen::CallFunc(llvm::Value *fn,
                               const std::vector<llvm::Value *> &args) const {
  return GetBuilder().CreateCall(fn, args);
}

llvm::Value *CodeGen::CallMalloc(uint32_t size, uint32_t alignment) {
  llvm::Value *mem_ptr = nullptr;
  if (alignment == 0) {
    auto *malloc_fn = LookupFunction("malloc");
    if (malloc_fn == nullptr) {
      malloc_fn = RegisterFunction(
          "malloc",
          llvm::TypeBuilder<void *(size_t), false>::get(GetContext()));
    }
    mem_ptr = CallFunc(malloc_fn, {Const64(size)});
  } else {
    // Caller wants an aligned malloc
    auto *aligned_alloc_fn = LookupFunction("aligned_alloc");
    if (aligned_alloc_fn == nullptr) {
      aligned_alloc_fn = RegisterFunction(
          "aligned_alloc",
          llvm::TypeBuilder<void *(size_t, size_t), false>::get(GetContext()));
    }
    mem_ptr = CallFunc(aligned_alloc_fn, {Const64(size), Const64(alignment)});
  }

  // Zero-out the allocated space
  GetBuilder().CreateMemSet(mem_ptr, Null(ByteType()), size, alignment);

  // We're done
  return mem_ptr;
}

llvm::Value *CodeGen::CallFree(llvm::Value *ptr) {
  auto *free_fn = LookupFunction("free");
  if (free_fn == nullptr) {
    free_fn = RegisterFunction(
        "free", llvm::TypeBuilder<void(void *), false>::get(GetContext()));
  }
  return CallFunc(free_fn, {ptr});
}

llvm::Value *CodeGen::CallPrintf(const std::string &format,
                                 const std::vector<llvm::Value *> &args) {
  auto *printf_fn = LookupFunction("printf");
  if (printf_fn == nullptr) {
    printf_fn = RegisterFunction(
        "printf",
        llvm::TypeBuilder<int(char *, ...), false>::get(GetContext()));
  }
  auto &ir_builder = code_context_.GetBuilder();
  auto *format_str =
      ir_builder.CreateGEP(ConstString(format), {Const32(0), Const32(0)});

  // Collect all the arguments into a vector
  std::vector<llvm::Value *> printf_args{format_str};
  printf_args.insert(printf_args.end(), args.begin(), args.end());

  // Call the function
  return CallFunc(printf_fn, printf_args);
}

llvm::Value *CodeGen::CallAddWithOverflow(llvm::Value *left, llvm::Value *right,
                                          llvm::Value *&overflow_bit) const {
  PL_ASSERT(left->getType() == right->getType());

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
                                          llvm::Value *&overflow_bit) const {
  PL_ASSERT(left->getType() == right->getType());

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
                                          llvm::Value *&overflow_bit) const {
  PL_ASSERT(left->getType() == right->getType());
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
  PL_ASSERT(overflow->getType() == BoolType());

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
  PL_ASSERT(divide_by_zero->getType() == BoolType());

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

// Lookup a function in the module with the given name
llvm::Function *CodeGen::LookupFunction(const std::string &fn_name) const {
  return GetModule().getFunction(fn_name);
}

// Register the given function symbol and the LLVM function type it represents
llvm::Function *CodeGen::RegisterFunction(const std::string &fn_name,
                                          llvm::FunctionType *fn_type) {
  // Check if this is already registered as a built in, quit if to
  auto *builtin = LookupFunction(fn_name);
  if (builtin != nullptr) {
    return builtin;
  }

  // TODO: Function attributes here
  // We need to create an LLVM function for this guy
  auto &module = GetModule();
  auto *fn = llvm::Function::Create(fn_type, llvm::Function::ExternalLinkage,
                                    fn_name, &module);
  return fn;
}

llvm::Type *CodeGen::LookupTypeByName(const std::string &name) const {
  return GetModule().getTypeByName(name);
}

uint64_t CodeGen::SizeOf(llvm::Type *type) const {
  auto size = code_context_.GetDataLayout().getTypeSizeInBits(type) / 8;
  return size != 0 ? size : 1;
}

}  // namespace codegen
}  // namespace peloton
