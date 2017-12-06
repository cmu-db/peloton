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

#include <utility>

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
                                                  llvm::Type *type,
                                                  bool is_on_stack) {
  PL_ASSERT(constructed_type_ == nullptr);
  StateID state_id = static_cast<StateID>(state_slots_.size());
  StateInfo state_info = {
      .name = std::move(name),
      .type = type,
      .local = is_on_stack,
      .index = 0,
      .val = nullptr
  };
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

  PL_ASSERT(!state_info.local);

  // We index into the runtime state to get a pointer to the state
  std::string ptr_name{state_info.name + "Ptr"};
  llvm::Value *runtime_state = codegen.GetState();
  llvm::Value *state_ptr = codegen->CreateConstInBoundsGEP2_32(
      constructed_type_, runtime_state, 0, state_info.index, ptr_name);
  return state_ptr;
}

llvm::Value *RuntimeState::LoadStateValue(
    CodeGen &codegen, RuntimeState::StateID state_id) const {
  auto &state_info = state_slots_[state_id];
  if (state_info.local) {
    if (state_info.val == nullptr) {
      if (auto *arr_type = llvm::dyn_cast<llvm::ArrayType>(state_info.type)) {
        // Do the stack allocation of the array
        llvm::AllocaInst *arr = codegen->CreateAlloca(
            arr_type->getArrayElementType(),
            codegen.Const32(arr_type->getArrayNumElements()));

        // Set the alignment
        arr->setAlignment(Vector::kDefaultVectorAlignment);

        // Zero-out the allocated space
        uint64_t sz = codegen.SizeOf(state_info.type);
        codegen->CreateMemSet(arr, codegen.Const8(0), sz, arr->getAlignment());

        state_info.val = arr;
      } else {
        state_info.val = codegen->CreateAlloca(state_info.type);
      }

      // Set the name of the local state to what the client wants
      state_info.val->setName(state_info.name);
    }
    return state_info.val;
  }

  llvm::Value *state_ptr = LoadStatePtr(codegen, state_id);
  llvm::Value *state = codegen->CreateLoad(state_ptr);
#ifndef NDEBUG
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

  // Construct a type capturing all non-local state
  std::vector<llvm::Type *> types;
  for (uint32_t i = 0, index = 0; i < state_slots_.size(); i++) {
    if (!state_slots_[i].local) {
      // We set the index in the overall state where this instance is found
      state_slots_[i].index = index++;
      types.push_back(state_slots_[i].type);
    }
  }

  constructed_type_ =
      llvm::StructType::create(codegen.GetContext(), types, "RuntimeState");
  return constructed_type_;
}

}  // namespace codegen
}  // namespace peloton