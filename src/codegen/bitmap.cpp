//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bitmap.cpp
//
// Identification: src/codegen/bitmap.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/bitmap.h"

namespace peloton {
namespace codegen {

peloton::codegen::Bitmap::Bitmap(CodeGen &codegen, llvm::Value *bitmap_addr,
                                 uint32_t num_bits)
    : bitmap_addr_(bitmap_addr) {
  // Cast to byte array
  bitmap_addr_ =
      codegen->CreateBitOrPointerCast(bitmap_addr_, codegen.CharPtrType());

  // Resize and clear the cache
  uint64_t num_components = (num_bits + 7) / 8;
  cached_components_.resize(num_components, nullptr);
}

uint32_t Bitmap::ComponentFor(CodeGen &codegen, uint32_t bit_idx) {
  uint32_t pos = static_cast<uint32_t>(bit_idx / 8);

  // If the component at the position isn't loaded yet, load it now
  if (cached_components_[pos] == nullptr) {
    auto *component_addr = codegen->CreateConstInBoundsGEP1_32(
        codegen.Int8Type(), bitmap_addr_, pos);
    cached_components_[pos] = codegen->CreateLoad(component_addr);
  }

  return pos;
}

void peloton::codegen::Bitmap::SetBit(CodeGen &codegen, uint32_t bit_idx) {
  // Load the component the bit resides in
  uint32_t pos = ComponentFor(codegen, bit_idx);
  llvm::Value *component = cached_components_[pos];

  // Set the appropriate bit
  llvm::Value *mask = MaskFor(codegen, bit_idx);
  llvm::Value *modified_component = codegen->CreateOr(component, mask);

  // Store the cached component (don't write it back to memory yet)
  cached_components_[pos] = modified_component;
}

void Bitmap::SwitchBit(CodeGen &codegen, uint32_t bit_idx,
                       llvm::Value *bit_val) {
  PL_ASSERT(bit_val->getType() == codegen.BoolType());

  // Load the component the bit resides in
  uint32_t pos = ComponentFor(codegen, bit_idx);
  llvm::Value *component = cached_components_[pos];

  // First clear the bit
  llvm::Value *clear_mask = codegen->CreateNot(MaskFor(codegen, bit_idx));
  llvm::Value *cleared_component = codegen->CreateAnd(component, clear_mask);

  // Set the bit to what is provided
  llvm::Value *mask = codegen->CreateZExt(bit_val, codegen.Int8Type());
  llvm::Value *modified_component = codegen->CreateOr(
      cleared_component, codegen->CreateShl(mask, bit_idx & 7));

  // Store the cached component (don't write it back to memory yet)
  cached_components_[pos] = modified_component;
}

void peloton::codegen::Bitmap::ClearBit(CodeGen &codegen, uint32_t bit_idx) {
  // Load the component the bit resides in
  uint32_t pos = ComponentFor(codegen, bit_idx);
  llvm::Value *component = cached_components_[pos];

  // Compute the complement of the mask in order to unset the bit
  llvm::Value *mask = codegen->CreateNot(MaskFor(codegen, bit_idx));
  llvm::Value *modified_component = codegen->CreateAnd(component, mask);

  // Store the cached component (don't write it back to memory yet)
  cached_components_[pos] = modified_component;
}

llvm::Value *Bitmap::GetBit(CodeGen &codegen, uint32_t bit_idx) {
  // Get the current/latest value of the component this bit belongs to
  uint32_t pos = ComponentFor(codegen, bit_idx);
  llvm::Value *component = cached_components_[pos];

  // Pull out only the bit we're interested in
  llvm::Value *mask = MaskFor(codegen, bit_idx);
  llvm::Value *res = codegen->CreateAnd(component, mask);

  // Need to convert res into a boolean type
  return codegen->CreateICmpNE(res, codegen.Const8(0));
}

void peloton::codegen::Bitmap::WriteBack(CodeGen &codegen) const {
  for (uint32_t i = 0; i < cached_components_.size(); i++) {
    if (cached_components_[i] != nullptr) {
      // We have a dirty component, write it back
      auto *component_addr = codegen->CreateConstInBoundsGEP1_32(
          codegen.Int8Type(), bitmap_addr_, i);
      codegen->CreateStore(cached_components_[i], component_addr);
    }
  }
}

}  // namespace codegen
}  // namespace peloton