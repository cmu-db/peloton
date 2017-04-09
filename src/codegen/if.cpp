//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// if.cpp
//
// Identification: src/codegen/if.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/if.h"

#include "llvm/Transforms/Utils/BasicBlockUtils.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Constructor. We create two BB's, one "then" BB that appears right after the
// conditional branch
//===----------------------------------------------------------------------===//
If::If(CodeGen &cg, llvm::Value *if_condition, std::string name)
    : cg_(cg),
      fn_(cg_->GetInsertBlock()->getParent()),
      last_bb_in_then_(nullptr),
      else_bb_(nullptr),
      last_bb_in_else_(nullptr) {
  then_bb_ = llvm::BasicBlock::Create(cg_.GetContext(), name, fn_);
  merge_bb_ = llvm::BasicBlock::Create(cg_.GetContext(), "ifCont");

  // This instruction needs to be saved in case we have an else block
  branch_ = cg_->CreateCondBr(if_condition, then_bb_, merge_bb_);
  cg_->SetInsertPoint(then_bb_);
}

void If::EndIf(llvm::BasicBlock *end_bb) {
  if (end_bb != nullptr) {
    // Create an unconditional branch to the provided basic block
    cg_->CreateBr(end_bb);
  } else {
    // Create an unconditional branch to the merging block
    cg_->CreateBr(merge_bb_);
  }

  auto *curr_bb = cg_->GetInsertBlock();
  if (else_bb_ == nullptr) {
    // There was no else block, the current block is the last in the "then"
    last_bb_in_then_ = curr_bb;
  } else {
    // There was an else block, the current block is the last in the "else"
    last_bb_in_else_ = curr_bb;
  }

  // Append the merge block to the end, and set it as the new insertion point
  fn_->getBasicBlockList().push_back(merge_bb_);
  cg_->SetInsertPoint(merge_bb_);
}

void If::ElseBlock(std::string name) {
  // Create an unconditional branch from where we are (from the 'then' branch)
  // to the merging block
  cg_->CreateBr(merge_bb_);

  // Create a new else block
  else_bb_ = llvm::BasicBlock::Create(cg_.GetContext(), name, fn_);
  last_bb_in_else_ = else_bb_;

  // Replace the previous branch instruction that normally went to the merging
  // block on a false predicate to now branch into the new else block
  auto *new_branch =
      llvm::BranchInst::Create(then_bb_, else_bb_, branch_->getCondition());
  ReplaceInstWithInst(branch_, new_branch);

  last_bb_in_then_ = cg_->GetInsertBlock();

  cg_->SetInsertPoint(else_bb_);
}

codegen::Value If::BuildPHI(codegen::Value v1, codegen::Value v2) {
  if (cg_->GetInsertBlock() != merge_bb_) {
    // The if hasn't been ended, end it now
    EndIf();
  }
  PL_ASSERT(v1.GetType() == v2.GetType());
  std::vector<std::pair<codegen::Value, llvm::BasicBlock *>> vals = {
      {v1, last_bb_in_then_},
      {v2,
       last_bb_in_else_ != nullptr ? last_bb_in_else_ : branch_->getParent()}};
  return codegen::Value::BuildPHI(cg_, vals);
}

llvm::Value *If::BuildPHI(llvm::Value *v1, llvm::Value *v2) {
  PL_ASSERT(v1->getType() == v2->getType());
  llvm::PHINode *phi = cg_->CreatePHI(v1->getType(), 2);
  phi->addIncoming(v1, last_bb_in_then_);
  phi->addIncoming(v2, last_bb_in_else_ != nullptr ? last_bb_in_else_
                                                   : branch_->getParent());
  return phi;
}

}  // namespace codegen
}  // namespace peloton