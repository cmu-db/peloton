//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_builder.cpp
//
// Identification: src/codegen/function_builder.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/function_builder.h"

#include "codegen/proxy/runtime_functions_proxy.h"
#include "common/exception.h"

namespace peloton {
namespace codegen {

// We preserve the state of any ongoing function construction in order to be
// able to restore it after this function has been fully completed. Thus,
// FunctionBuilders are nestable, allowing the definition of a function to begin
// while in midst of defining another function.
FunctionBuilder::FunctionBuilder(
    CodeContext &code_context, std::string name, llvm::Type *ret_type,
    const std::vector<std::pair<std::string, llvm::Type *>> &args)
    : finished_(false),
      code_context_(code_context),
      previous_function_(code_context_.GetCurrentFunction()),
      previous_insert_point_(code_context_.GetBuilder().GetInsertBlock()),
      overflow_bb_(nullptr),
      divide_by_zero_bb_(nullptr) {
  // Collect function argument types
  std::vector<llvm::Type *> arg_types;
  for (auto &arg : args) {
    arg_types.push_back(arg.second);
  }

  // Declare the function
  auto *fn_type = llvm::FunctionType::get(ret_type, arg_types, false);
  func_ = llvm::Function::Create(fn_type, llvm::Function::ExternalLinkage, name,
                                 &code_context_.GetModule());

  // Set the argument names
  auto arg_iter = args.begin();
  for (auto iter = func_->arg_begin(), end = func_->arg_end(); iter != end;
       iter++, arg_iter++) {
    iter->setName(arg_iter->first);
  }

  // Set the entry point of the function
  entry_bb_ =
      llvm::BasicBlock::Create(code_context_.GetContext(), "entry", func_);
  code_context_.GetBuilder().SetInsertPoint(entry_bb_);
  code_context_.SetCurrentFunction(this);

  // Register the function we're creating with the code context
  code_context_.RegisterFunction(func_);
}

// When we destructing the FunctionBuilder, we just do a sanity check to ensure
// that the user actually finished constructing the function. This is because we
// do cleanup in Finish().
FunctionBuilder::~FunctionBuilder() { PL_ASSERT(finished_); }

// Here, we just need to iterate over the arguments in the function to find a
// match. The names of the arguments were provided and set at construction time.
llvm::Value *FunctionBuilder::GetArgumentByName(std::string name) {
  for (auto &arg : func_->getArgumentList()) {
    if (arg.getName().equals(name)) {
      return &arg;
    }
  }
  PL_ASSERT(false);
  return nullptr;
}

llvm::Value *FunctionBuilder::GetArgumentByPosition(uint32_t index) {
  PL_ASSERT(index < func_->arg_size());
  uint32_t pos = 0;
  for (auto arg_iter = func_->arg_begin(), end = func_->arg_end();
       arg_iter != end; ++arg_iter, ++pos) {
    if (pos == index) {
      return &*arg_iter;
    }
  }
  PL_ASSERT(false);
  return nullptr;
}

// Every function has a dedicated basic block where overflow exceptions are
// thrown. This is so that this exception code isn't duplicated across the
// function for every overflow check. Instead, it's expected that any time an
// overflow is detected, a jump is made into this function's overflow block. The
// contents of the block are just a call into the runtime functions that throws
// the actual exception.
llvm::BasicBlock *FunctionBuilder::GetOverflowBB() {
  // Return the block if it has already been created
  if (overflow_bb_ != nullptr) {
    return overflow_bb_;
  }

  CodeGen codegen{code_context_};

  // Save the current position so we can restore after we're done
  auto *curr_position = codegen->GetInsertBlock();

  // Create the overflow block now, but don't attach to function just yet.
  overflow_bb_ = llvm::BasicBlock::Create(codegen.GetContext(), "overflow");

  // Make a call into RuntimeFunctions::ThrowOverflowException()
  codegen->SetInsertPoint(overflow_bb_);
  codegen.Call(RuntimeFunctionsProxy::ThrowOverflowException, {});
  codegen->CreateUnreachable();

  // Restore position
  codegen->SetInsertPoint(curr_position);

  return overflow_bb_;
}

// Similar to the overflow basic block, every function has a dedicated basic
// block where divide-by-zero exceptions are thrown. This function constructs
// the basic block if it hasn't been created before. The contents of the block
// are just a call into the runtime functions that throws the actual exception.
llvm::BasicBlock *FunctionBuilder::GetDivideByZeroBB() {
  // Return the block if it has already been created
  if (divide_by_zero_bb_ != nullptr) {
    return divide_by_zero_bb_;
  }

  CodeGen codegen{code_context_};

  // Save the current position so we can restore after we're done
  auto *curr_position = codegen->GetInsertBlock();

  // Create the overflow block now, but don't attach to function just yet.
  divide_by_zero_bb_ =
      llvm::BasicBlock::Create(codegen.GetContext(), "divideByZero");

  // Make a call into RuntimeFunctions::ThrowDivideByZeroException()
  codegen->SetInsertPoint(divide_by_zero_bb_);
  codegen.Call(RuntimeFunctionsProxy::ThrowDivideByZeroException, {});
  codegen->CreateUnreachable();

  // Restore position
  codegen->SetInsertPoint(curr_position);

  return divide_by_zero_bb_;
}

// Return the given value from the function and finish it
void FunctionBuilder::ReturnAndFinish(llvm::Value *ret) {
  if (!finished_) {
    if (ret != nullptr) {
      code_context_.GetBuilder().CreateRet(ret);
    } else {
      code_context_.GetBuilder().CreateRetVoid();
    }

    // Add the overflow error block if it exists
    if (overflow_bb_ != nullptr) {
      overflow_bb_->insertInto(func_);
    }

    // Add the divide-by-zero error block if it exists
    if (divide_by_zero_bb_ != nullptr) {
      divide_by_zero_bb_->insertInto(func_);
    }

    // Restore previous function construction state in the code context
    if (previous_insert_point_ != nullptr) {
      PL_ASSERT(previous_function_ != nullptr);
      code_context_.GetBuilder().SetInsertPoint(previous_insert_point_);
      code_context_.SetCurrentFunction(previous_function_);
    }

    // Now we're done
    finished_ = true;
  }
}

}  // namespace codegen
}  // namespace peloton
