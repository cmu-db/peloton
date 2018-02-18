//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter.cpp
//
// Identification: src/codegen/sorter.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/sorter.h"

#include "codegen/lang/loop.h"
#include "codegen/proxy/sorter_proxy.h"
#include "codegen/lang/vectorized_loop.h"
#include "codegen/vector.h"

namespace peloton {
namespace codegen {

Sorter::Sorter() {
  // This constructor shouldn't generally be used at all, but there are
  // cases when the tuple description is not known fully at construction time.
}

Sorter::Sorter(CodeGen &codegen, const std::vector<type::Type> &row_desc) {
  // Configure the storage format using the provided row description
  for (const auto &value_type : row_desc) {
    storage_format_.AddType(value_type);
  }
  // Finalize the format
  storage_format_.Finalize(codegen);
}

void Sorter::Init(CodeGen &codegen, llvm::Value *sorter_ptr,
                  llvm::Value *comparison_func) const {
  auto *tuple_size = codegen.Const32(storage_format_.GetStorageSize());
  codegen.Call(SorterProxy::Init, {sorter_ptr, comparison_func, tuple_size});
}

void Sorter::Append(CodeGen &codegen, llvm::Value *sorter_ptr,
                    const std::vector<codegen::Value> &tuple) const {
  // First, call Sorter::StoreInputTuple() to get a handle to a contiguous
  // chunk of free space large enough to materialize a single tuple.
  auto *space = codegen.Call(SorterProxy::StoreInputTuple, {sorter_ptr});

  // Now, individually store the attributes of the tuple into the free space
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_format_, space);
  for (uint32_t col_id = 0; col_id < tuple.size(); col_id++) {
    storage_format_.SetValue(codegen, space, col_id, tuple[col_id],
                             null_bitmap);
  }
  null_bitmap.WriteBack(codegen);
}

void Sorter::Sort(CodeGen &codegen, llvm::Value *sorter_ptr) const {
  codegen.Call(SorterProxy::Sort, {sorter_ptr});
}

void Sorter::SortParallel(CodeGen &codegen, llvm::Value *sorter_ptr,
                          llvm::Value *thread_states,
                          uint32_t sorter_offset) const {
  auto *offset = codegen.Const32(sorter_offset);
  codegen.Call(SorterProxy::SortParallel, {sorter_ptr, thread_states, offset});
}

void Sorter::Iterate(CodeGen &codegen, llvm::Value *sorter_ptr,
                     Sorter::IterateCallback &callback) const {
  struct TaatIterateCallback : VectorizedIterateCallback {
    const UpdateableStorage &storage;
    Sorter::IterateCallback &callback;

    TaatIterateCallback(const UpdateableStorage &s, Sorter::IterateCallback &c)
        : storage(s), callback(c) {}

    void ProcessEntries(CodeGen &codegen, llvm::Value *start_index,
                        llvm::Value *end_index, SorterAccess &access) const {
      lang::Loop loop(codegen, codegen->CreateICmpULT(start_index, end_index),
                      {{"start", start_index}});
      {
        llvm::Value *curr_index = loop.GetLoopVar(0);

        // Parse the row
        std::vector<codegen::Value> vals;
        auto &row = access.GetRow(curr_index);
        for (uint32_t i = 0; i < storage.GetNumElements(); i++) {
          vals.emplace_back(row.LoadColumn(codegen, i));
        }

        // Call the actual callback
        callback.ProcessEntry(codegen, vals);

        curr_index = codegen->CreateAdd(curr_index, codegen.Const32(1));
        loop.LoopEnd(codegen->CreateICmpULT(curr_index, end_index),
                     {curr_index});
      }
    }
  };

  // Do a vectorized iteration using our callback adapter
  TaatIterateCallback taat_cb(GetStorageFormat(), callback);
  VectorizedIterate(codegen, sorter_ptr, Vector::kDefaultVectorSize, taat_cb);
}

// Iterate over the tuples in the sorter in batches/vectors of the given size
void Sorter::VectorizedIterate(
    CodeGen &codegen, llvm::Value *sorter_ptr, uint32_t vector_size,
    Sorter::VectorizedIterateCallback &callback) const {
  llvm::Value *start_pos = GetStartPosition(codegen, sorter_ptr);
  llvm::Value *num_tuples = NumTuples(codegen, sorter_ptr);
  num_tuples = codegen->CreateTrunc(num_tuples, codegen.Int32Type());
  lang::VectorizedLoop loop(codegen, num_tuples, vector_size, {});
  {
    // Current loop range
    auto curr_range = loop.GetCurrentRange();

    // Provide an accessor into the sorted space
    SorterAccess sorter_access(*this, start_pos);

    // Issue the callback
    callback.ProcessEntries(codegen, curr_range.start, curr_range.end,
                            sorter_access);

    // That's it
    loop.LoopEnd(codegen, {});
  }
}

void Sorter::Destroy(CodeGen &codegen, llvm::Value *sorter_ptr) const {
  codegen.Call(SorterProxy::Destroy, {sorter_ptr});
}

llvm::Value *Sorter::NumTuples(CodeGen &codegen,
                               llvm::Value *sorter_ptr) const {
  // Pull out start and end (char **)
  auto *start = GetStartPosition(codegen, sorter_ptr);
  auto *end = GetEndPosition(codegen, sorter_ptr);

  // Convert both to uint64_t
  start = codegen->CreatePtrToInt(start, codegen.Int64Type());
  end = codegen->CreatePtrToInt(end, codegen.Int64Type());

  // Subtract (end - start)
  auto *diff = codegen->CreateSub(end, start);

  // Divide by pointer size (diff >> 3, or div / 8)
  return codegen->CreateAShr(diff, 3, "numTuples", true);
}

////////////////////////////////////////////////////////////////////////////////
/// TODO: We should codify access to instance memory variables using templates
///       to something like: codegen.LoadMember<SorterProxy::start_pos>(...)
////////////////////////////////////////////////////////////////////////////////

llvm::Value *Sorter::GetStartPosition(CodeGen &codegen,
                                      llvm::Value *sorter_ptr) const {
  auto *sorter_type = SorterProxy::GetType(codegen);
  auto *start_ptr_addr =
      codegen->CreateConstInBoundsGEP2_32(sorter_type, sorter_ptr, 0, 1);
  return codegen->CreateLoad(start_ptr_addr);
}

llvm::Value *Sorter::GetEndPosition(CodeGen &codegen,
                                    llvm::Value *sorter_ptr) const {
  auto *sorter_type = SorterProxy::GetType(codegen);
  auto *end_ptr_addr =
      codegen->CreateConstInBoundsGEP2_32(sorter_type, sorter_ptr, 0, 2);
  return codegen->CreateLoad(end_ptr_addr);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Sorter Access
///
////////////////////////////////////////////////////////////////////////////////

Sorter::SorterAccess::SorterAccess(const Sorter &sorter, llvm::Value *start_pos)
    : sorter_(sorter), start_pos_(start_pos) {}

Sorter::SorterAccess::Row &Sorter::SorterAccess::GetRow(llvm::Value *row_idx) {
  auto iter = cached_rows_.find(row_idx);
  if (iter == cached_rows_.end()) {
    cached_rows_.emplace(row_idx, Row(*this, row_idx));
    iter = cached_rows_.find(row_idx);
  }
  return iter->second;
}

codegen::Value Sorter::SorterAccess::LoadRowValue(
    CodeGen &codegen, Sorter::SorterAccess::Row &row,
    uint32_t column_index) const {
  if (row.row_pos_ == nullptr) {
    auto *addr = codegen->CreateInBoundsGEP(codegen.CharPtrType(), start_pos_,
                                            row.row_idx_);
    row.row_pos_ = codegen->CreateLoad(addr);
  }

  const auto &storage_format = sorter_.GetStorageFormat();
  UpdateableStorage::NullBitmap null_bitmap(codegen, storage_format,
                                            row.row_pos_);
  return storage_format.GetValue(codegen, row.row_pos_, column_index,
                                 null_bitmap);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Sorter Access - Row
///
////////////////////////////////////////////////////////////////////////////////

Sorter::SorterAccess::Row::Row(SorterAccess &access, llvm::Value *row_index)
    : access_(access), row_idx_(row_index), row_pos_(nullptr) {}

codegen::Value Sorter::SorterAccess::Row::LoadColumn(CodeGen &codegen,
                                                     uint32_t column_index) {
  return access_.LoadRowValue(codegen, *this, column_index);
}

}  // namespace codegen
}  // namespace peloton