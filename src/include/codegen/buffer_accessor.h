//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffer_accessor.h
//
// Identification: src/include/codegen/buffer_accessor.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "codegen/codegen.h"
#include "codegen/updateable_storage.h"

namespace peloton {
namespace codegen {

class BufferAccessor {
 public:
  BufferAccessor();
  BufferAccessor(CodeGen &codegen, const std::vector<type::Type> &tuple_desc);

  void Init(CodeGen &codegen, llvm::Value *buffer_ptr) const;

  void Append(CodeGen &codegen, llvm::Value *buffer_ptr,
              const std::vector<codegen::Value> &tuple) const;

  struct IterateCallback;
  void Iterate(CodeGen &codegen, llvm::Value *buffer_ptr,
               IterateCallback &callback) const;

  void Reset(CodeGen &codegen, llvm::Value *buffer_ptr) const;

  void Destroy(CodeGen &codegen, llvm::Value *buffer_ptr) const;

  llvm::Value *NumTuples(CodeGen &codegen, llvm::Value *buffer_ptr) const;

  uint32_t GetTupleSize() const { return storage_format_.GetStorageSize(); }

  struct IterateCallback {
    virtual void ProcessEntry(
        CodeGen &codegen, const std::vector<codegen::Value> &vals) const = 0;
  };

 private:
  UpdateableStorage storage_format_;
};

}  // namespace codegen
}  // namespace peloton