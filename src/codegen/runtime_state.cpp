//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_state.cpp
//
// Identification: src/codegen/runtime_state.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/runtime_state.h"
#include "codegen/vector.h"

namespace peloton {
namespace codegen {

// Constructor
RuntimeState::RuntimeState() : constructed_type_(nullptr) {}

// Register some state of the given type and with the given name. The last
// argument indicates whether this state is local (i.e., lives on the stack) or
// whether the requesting operator wants to manage the memory.
RuntimeState::StateID RuntimeState::RegisterState(std::string name,
                                                  llvm::Type *type) {
  PL_ASSERT(constructed_type_ == nullptr);
  RuntimeState::StateID state_id = state_slots_.size();
  RuntimeState::StateInfo state_info;
  state_info.name = name;
  state_info.type = type;
  state_slots_.push_back(state_info);
  return state_id;
}

llvm::Value *RuntimeState::LoadStatePtr(CodeGen &codegen,
                                        RuntimeState::StateID state_id) const {
  // At this point, the runtime state type must have been finalized. Otherwise,
  // it'd be impossible for us to index into it because the type would be
  // incomplete.
  PL_ASSERT(constructed_type_ != nullptr);
  PL_ASSERT(state_id < state_slots_.size());

  auto &state_info = state_slots_[state_id];

  // We index into the runtime state to get a pointer to the state
  std::string ptr_name{state_info.name + "Ptr"};
  llvm::Value *runtime_state = codegen.GetState();
  llvm::Value *state_ptr = codegen->CreateConstInBoundsGEP2_32(
      constructed_type_, runtime_state, 0, state_info.index, ptr_name);
  return state_ptr;
}

llvm::Value *RuntimeState::LoadStateValue(
    CodeGen &codegen, RuntimeState::StateID state_id) const {
  llvm::Value *state_ptr = LoadStatePtr(codegen, state_id);
  llvm::Value *state = codegen->CreateLoad(state_ptr);
#ifndef NDEBUG
  auto &state_info = state_slots_[state_id];
  PL_ASSERT(state->getType() == state_info.type);
  if (state->getType()->isStructTy()) {
    PL_ASSERT(state_info.type->isStructTy());
    auto *our_type = llvm::cast<llvm::StructType>(state_info.type);
    auto *ret_type = llvm::cast<llvm::StructType>(state->getType());
    PL_ASSERT(ret_type->isLayoutIdentical(our_type));
  }
#endif
  return state;
}

llvm::Type *RuntimeState::FinalizeType(CodeGen &codegen) {
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
      llvm::StructType::create(codegen.GetContext(), types, "RuntimeState");
  return constructed_type_;
}

}  // namespace codegen
}  // namespace peloton