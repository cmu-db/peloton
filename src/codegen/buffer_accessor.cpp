//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffer_accessor.cpp
//
// Identification: src/codegen/buffer_accessor.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/buffer_accessor.h"

#include "codegen/lang/loop.h"
#include "codegen/proxy/buffer_proxy.h"

namespace peloton {
namespace codegen {

BufferAccessor::BufferAccessor() = default;

BufferAccessor::BufferAccessor(CodeGen &codegen,
                               const std::vector<type::Type> &tuple_desc) {
  for (const auto &value_type : tuple_desc) {
    storage_format_.AddType(value_type);
  }
  storage_format_.Finalize(codegen);
}

void BufferAccessor::Init(CodeGen &codegen, llvm::Value *buffer_ptr) const {
  codegen.Call(BufferProxy::Init, {buffer_ptr});
}

void BufferAccessor::Append(CodeGen &codegen, llvm::Value *buffer_ptr,
                            const std::vector<codegen::Value> &tuple) const {
  auto *size = codegen.Const32(storage_format_.GetStorageSize());
  auto *space = codegen.Call(BufferProxy::Append, {buffer_ptr, size});

  // Now, individually store the attributes of the tuple into the free space
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_format_, space);
  for (uint32_t col_id = 0; col_id < tuple.size(); col_id++) {
    storage_format_.SetValue(codegen, space, col_id, tuple[col_id],
                             null_bitmap);
  }
  null_bitmap.WriteBack(codegen);
}

void BufferAccessor::Iterate(CodeGen &codegen, llvm::Value *buffer_ptr,
                             BufferAccessor::IterateCallback &callback) const {
  auto *start = codegen.Load(BufferProxy::buffer_start, buffer_ptr);
  auto *end = codegen.Load(BufferProxy::buffer_pos, buffer_ptr);
  lang::Loop loop(codegen, codegen->CreateICmpNE(start, end), {{"pos", start}});
  {
    auto *pos = loop.GetLoopVar(0);

    // Read
    std::vector<codegen::Value> vals;
    UpdateableStorage::NullBitmap null_bitmap(codegen, storage_format_, pos);
    for (uint32_t col_id = 0; col_id < storage_format_.GetNumElements();
         col_id++) {
      auto val = storage_format_.GetValue(codegen, pos, col_id, null_bitmap);
      vals.emplace_back(val);
    }

    // Invoke callback
    callback.ProcessEntry(codegen, vals);

    // Move along
    auto *next = codegen->CreateConstInBoundsGEP1_64(
        pos, storage_format_.GetStorageSize());
    loop.LoopEnd(codegen->CreateICmpNE(next, end), {next});
  }
}

void BufferAccessor::Reset(CodeGen &codegen, llvm::Value *buffer_ptr) const {
  codegen.Call(BufferProxy::Reset, {buffer_ptr});
}

void BufferAccessor::Destroy(CodeGen &codegen, llvm::Value *buffer_ptr) const {
  codegen.Call(BufferProxy::Destroy, {buffer_ptr});
}

llvm::Value *BufferAccessor::NumTuples(CodeGen &codegen,
                                       llvm::Value *buffer_ptr) const {
  auto *start = codegen.Load(BufferProxy::buffer_start, buffer_ptr);
  auto *end = codegen.Load(BufferProxy::buffer_pos, buffer_ptr);
  start = codegen->CreatePtrToInt(start, codegen.Int64Type());
  end = codegen->CreatePtrToInt(end, codegen.Int64Type());
  auto *diff = codegen->CreateSub(end, start);
  diff = codegen->CreateAShr(diff, 3, "numTuples", true);
  return codegen->CreateTrunc(diff, codegen.Int32Type());
}

}  // namespace codegen
}  // namespace peloton