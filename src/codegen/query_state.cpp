//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_state.cpp
//
// Identification: src/codegen/query_state.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_state.h"
#include "codegen/vector.h"

namespace peloton {
namespace codegen {

// Constructor
QueryState::QueryState() : constructed_type_(nullptr) {}

// Register some state of the given type and with the given name. The last
// argument indicates whether this state is local (i.e., lives on the stack) or
// whether the requesting operator wants to manage the memory.
QueryState::Id QueryState::RegisterState(std::string name, llvm::Type *type) {
  PELOTON_ASSERT(constructed_type_ == nullptr);
  QueryState::Id state_id = state_slots_.size();
  QueryState::StateInfo state_info;
  state_info.name = name;
  state_info.type = type;
  state_slots_.push_back(state_info);
  return state_id;
}

llvm::Value *QueryState::LoadStatePtr(CodeGen &codegen,
                                      QueryState::Id state_id) const {
  // At this point, the runtime state type must have been finalized. Otherwise,
  // it'd be impossible for us to index into it because the type would be
  // incomplete.
  PELOTON_ASSERT(constructed_type_ != nullptr);
  PELOTON_ASSERT(state_id < state_slots_.size());

  auto &state_info = state_slots_[state_id];

  // We index into the runtime state to get a pointer to the state
  std::string ptr_name{state_info.name + "Ptr"};
  llvm::Value *query_state = codegen.GetState();
  llvm::Value *state_ptr = codegen->CreateConstInBoundsGEP2_32(
      constructed_type_, query_state, 0, state_info.index, ptr_name);
  return state_ptr;
}

llvm::Value *QueryState::LoadStateValue(CodeGen &codegen,
                                        QueryState::Id state_id) const {
  llvm::Value *state_ptr = LoadStatePtr(codegen, state_id);
  llvm::Value *state = codegen->CreateLoad(state_ptr);
#ifndef NDEBUG
  auto &state_info = state_slots_[state_id];
  PELOTON_ASSERT(state->getType() == state_info.type);
  if (state->getType()->isStructTy()) {
    PELOTON_ASSERT(state_info.type->isStructTy());
    auto *our_type = llvm::cast<llvm::StructType>(state_info.type);
    auto *ret_type = llvm::cast<llvm::StructType>(state->getType());
    PELOTON_ASSERT(ret_type->isLayoutIdentical(our_type));
  }
#endif
  return state;
}

llvm::Type *QueryState::FinalizeType(CodeGen &codegen) {
  // Check if we've already constructed the type
  if (constructed_type_ != nullptr) {
    return constructed_type_;
  }

  // Construct a type capturing all state
  std::vector<llvm::Type *> types;
  for (uint32_t i = 0, index = 0; i < state_slots_.size(); i++) {
    // We set the index in the overall state where this instance is found
    state_slots_[i].index = index++;
    types.push_back(state_slots_[i].type);
  }

  constructed_type_ =
      llvm::StructType::create(codegen.GetContext(), types, "QueryState");
  return constructed_type_;
}

llvm::Type *QueryState::GetType() const {
  PELOTON_ASSERT(constructed_type_ != nullptr);
  return constructed_type_;
}

}  // namespace codegen
}  // namespace peloton