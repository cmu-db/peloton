//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bitmap.h
//
// Identification: src/include/codegen/bitmap.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A generic bitmap interface to treat an array of bytes as a bitmap.
//===----------------------------------------------------------------------===//
class Bitmap {
 public:
  Bitmap(CodeGen &codegen, llvm::Value *bitmap_addr, uint32_t num_bits);

  void SetBit(CodeGen &codegen, uint32_t bit_idx);

  void SwitchBit(CodeGen &codegen, uint32_t bit_idx, llvm::Value *bit_val);

  void ClearBit(CodeGen &codegen, uint32_t bit_idx);

  llvm::Value *GetBit(CodeGen &codegen, uint32_t bit_idx);

  void WriteBack(CodeGen &codegen) const;

  uint64_t NumComponents() const { return cached_components_.size(); }

  static uint64_t NumComponentsFor(uint64_t num_bits) {
    return (num_bits + 7) / 8;
  }

 private:
  // Return the component position where the given bit lives
  uint32_t ComponentFor(CodeGen &codegen, uint32_t bit_idx);

  // Returns a component with only the given (global) bit set
  llvm::Value *MaskFor(CodeGen &codegen, uint32_t bit_idx) const {
    return codegen.Const8((1 << (bit_idx & 7)));
  }

 private:
  // The memory address of the bitmap
  llvm::Value *bitmap_addr_;

  // We store cached components of the bitmap here to void redundant load/stores
  // when modifying bits in the same component
  std::vector<llvm::Value *> cached_components_;
};

}  // namespace codegen
}  // namespace peloton
