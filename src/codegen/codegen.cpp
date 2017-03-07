//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// codegen.cpp
//
// Identification: src/codegen/codegen.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen.h"

#include "llvm/ExecutionEngine/SectionMemoryManager.h"
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

llvm::Value *CodeGen::CallFunc(
    llvm::Value *fn, const std::initializer_list<llvm::Value *> args) const {
  std::vector<llvm::Value *> vector_args{args};
  return GetBuilder().CreateCall(fn, vector_args);
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

  // Call memset() to zero out everything
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

uint64_t CodeGen::SizeOf(llvm::Type *type) const {
  auto &data_layout = code_context_.GetDataLayout();
  return data_layout.getTypeSizeInBits(type) / 8;
}

}  // namespace codegen
}  // namespace peloton