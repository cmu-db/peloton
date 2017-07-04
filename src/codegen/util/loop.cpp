//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// loop.cpp
//
// Identification: src/codegen/loop.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "include/codegen/util/loop.h"

namespace peloton {
namespace codegen {
namespace util {

// Constructor
Loop::Loop(CodeGen &cg, llvm::Value *start_condition,
           const std::vector<Loop::LoopVariable> &loop_vars)
    : cg_(cg),
      fn_(cg_->GetInsertBlock()->getParent()),
      pre_loop_bb_(cg_->GetInsertBlock()),
      last_loop_bb_(nullptr) {
  // Create the loop block and the end block (outside loop)
  loop_bb_ = llvm::BasicBlock::Create(cg_.GetContext(), "loop", fn_);
  end_bb_ = llvm::BasicBlock::Create(cg_.GetContext(), "afterLoop");

  // Start loop only if condition is met
  cg_->CreateCondBr(start_condition, loop_bb_, end_bb_);
  cg_->SetInsertPoint(loop_bb_);

  // Create phi nodes for each loop variable
  for (auto &loop_var : loop_vars) {
    auto *phi_node = cg_->CreatePHI(loop_var.val->getType(), 2, loop_var.name);
    phi_node->addIncoming(loop_var.val, pre_loop_bb_);
    phi_nodes_.push_back(phi_node);
  }
}

llvm::Value *Loop::GetLoopVar(uint32_t id) const {
  // Get a specific loop variable
  return id < phi_nodes_.size() ? phi_nodes_[id] : nullptr;
}

// Function to mark the end of the loop. It ties up all the PHI nodes with
// their new values.
void Loop::LoopEnd(llvm::Value *end_condition,
                   const std::vector<llvm::Value *> &next) {
  // We have the ending BB, use that to add incoming to each PHI loop
  // variable
  auto *loop_end_bb = cg_->GetInsertBlock();
  last_loop_bb_ = loop_end_bb;
  cg_->CreateCondBr(end_condition, loop_bb_, end_bb_);

  for (uint32_t i = 0; i < phi_nodes_.size(); i++) {
    phi_nodes_[i]->addIncoming(next[i], loop_end_bb);
  }

  // Loop is complete, all new instructions go to the end BB
  fn_->getBasicBlockList().push_back(end_bb_);
  cg_->SetInsertPoint(end_bb_);
}

// Collect all the final values of the loop variables after the loop is
// complete
void Loop::CollectFinalLoopVariables(std::vector<llvm::Value *> &loop_vals) {
  PL_ASSERT(last_loop_bb_ != nullptr);
  for (const auto *phi_node : phi_nodes_) {
    llvm::PHINode *end_phi =
        cg_->CreatePHI(phi_node->getType(), 2, phi_node->getName() + "End");
    end_phi->addIncoming(phi_node->getIncomingValue(0), pre_loop_bb_);
    end_phi->addIncoming(phi_node->getIncomingValue(1), last_loop_bb_);
    loop_vals.push_back(end_phi);
  }
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton