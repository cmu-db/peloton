//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_builder.cpp
//
// Identification: src/codegen/function_builder.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/function_builder.h"

#include "include/codegen/proxy/runtime_functions_proxy.h"
#include "common/exception.h"

namespace peloton {
namespace codegen {

// In the constructor, we want to get the state of any previous function
// generation the given code_context is currently undergoing. We keep these
// around
// so that when the caller finishes constructing this function, we can restore
// the previous state nicely. This enables functions to be created while in the
// middle of creating others. In general, it presents a clean API that callers
// don't need to worry about IRBuilders, BasicBlocks and insertion points.
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
  llvm::FunctionType *fn_type =
      llvm::FunctionType::get(ret_type, arg_types, false);
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
}

// When we destructing the FunctionBuilder, we just do a sanity check to ensure
// that the user actually finished constructing the function. This is because we
// do cleanup in Finish().
FunctionBuilder::~FunctionBuilder() {
  if (!finished_) {
    throw Exception{"FunctionBuilder::Finish() was not called!"};
  }
}

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
  for (auto arg = func_->arg_begin(), end = func_->arg_end(); arg != end;
       ++arg) {
    if (pos++ == index) {
      return arg;
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
  // Return the basic block if it's already been created
  if (overflow_bb_ != nullptr) {
    return overflow_bb_;
  }

  // TODO: HACK - should this be a parameter?
  CodeGen codegen{code_context_};

  // Save the current position so we can restore after we're done
  auto *curr_position = codegen->GetInsertBlock();

  // Create the overflow block now, but don't attach to function just yet.
  overflow_bb_ = llvm::BasicBlock::Create(codegen.GetContext(), "overflow");

  // Make a call into RuntimeFunctions::ThrowOverflowException()
  codegen->SetInsertPoint(overflow_bb_);
  codegen.CallFunc(
      RuntimeFunctionsProxy::_ThrowOverflowException::GetFunction(codegen), {});
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
  // Return the block if it's already been created
  if (divide_by_zero_bb_ != nullptr) {
    return divide_by_zero_bb_;
  }

  // Create the block now

  CodeGen codegen{code_context_};

  // Save the current position so we can restore after we're done
  auto *curr_position = codegen->GetInsertBlock();

  // Create the overflow block now, but don't attach to function just yet.
  divide_by_zero_bb_ =
      llvm::BasicBlock::Create(codegen.GetContext(), "divideByZero");

  // Make a call into RuntimeFunctions::ThrowDivideByZeroException()
  codegen->SetInsertPoint(divide_by_zero_bb_);
  codegen.CallFunc(
      RuntimeFunctionsProxy::_ThrowDivideByZeroException::GetFunction(codegen),
      {});
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