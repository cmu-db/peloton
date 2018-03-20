//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter.cpp
//
// Identification: src/codegen/sorter.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
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

// Just make a call to util::Sorter::Init(...)
void Sorter::Init(CodeGen &codegen, llvm::Value *sorter_ptr,
                  llvm::Value *comparison_func) const {
  auto *tuple_size = codegen.Const32(storage_format_.GetStorageSize());
  codegen.Call(SorterProxy::Init, {sorter_ptr, comparison_func, tuple_size});
}

// Append the given tuple into the sorter instance
void Sorter::Append(CodeGen &codegen, llvm::Value *sorter_ptr,
                    const std::vector<codegen::Value> &tuple) const {
  // First, call Sorter::StoreInputTuple() to get a handle to a contiguous
  // chunk of free space large enough to materialize a single tuple.
  auto *space = codegen.Call(SorterProxy::StoreInputTuple, {sorter_ptr});

  // Now, individually store the attributes of the tuple into the free space
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_format_, space};
  for (uint32_t col_id = 0; col_id < tuple.size(); col_id++) {
    if (!null_bitmap.IsNullable(col_id)) {
      storage_format_.SetValueSkipNull(codegen, space, col_id, tuple[col_id]);
    } else {
      storage_format_.SetValue(codegen, space, col_id, tuple[col_id],
                               null_bitmap);
    }
  }
  null_bitmap.WriteBack(codegen);
}

// Just make a call to util::Sorter::Sort(...). This actually sorts the data
// that has been inserted into the sorter instance.
void Sorter::Sort(CodeGen &codegen, llvm::Value *sorter_ptr) const {
  //  auto *sort_func = SorterProxy::_Sort::GetFunction(codegen);
  codegen.Call(SorterProxy::Sort, {sorter_ptr});
}

void Sorter::Reset(CodeGen &codegen, llvm::Value *sorter_ptr) const {
  codegen.Call(SorterProxy::Clear, {sorter_ptr});
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
      lang::Loop loop{codegen,
                      codegen->CreateICmpULT(start_index, end_index),
                      {{"start", start_index}}};
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
  TaatIterateCallback taat_cb{GetStorageFormat(), callback};
  VectorizedIterate(codegen, sorter_ptr, Vector::kDefaultVectorSize, taat_cb);
}

// Iterate over the tuples in the sorter in batches/vectors of the given size
void Sorter::VectorizedIterate(
    CodeGen &codegen, llvm::Value *sorter_ptr, uint32_t vector_size,
    Sorter::VectorizedIterateCallback &callback) const {
  llvm::Value *start_pos = GetStartPosition(codegen, sorter_ptr);
  llvm::Value *num_tuples = GetNumberOfStoredTuples(codegen, sorter_ptr);
  lang::VectorizedLoop loop{codegen, num_tuples, vector_size, {}};
  {
    // Current loop range
    auto curr_range = loop.GetCurrentRange();

    // Provide an accessor into the sorted space
    SorterAccess sorter_access{*this, start_pos};

    // Issue the callback
    callback.ProcessEntries(codegen, curr_range.start, curr_range.end,
                            sorter_access);

    // That's it
    loop.LoopEnd(codegen, {});
  }
}

// Just make a call to util::Sorter::Destroy(...)
void Sorter::Destroy(CodeGen &codegen, llvm::Value *sorter_ptr) const {
  codegen.Call(SorterProxy::Destroy, {sorter_ptr});
}

llvm::Value *Sorter::GetNumberOfStoredTuples(CodeGen &codegen,
                                             llvm::Value *sorter_ptr) const {
  auto *sorter_type = SorterProxy::GetType(codegen);
  return codegen->CreateLoad(
      codegen->CreateConstInBoundsGEP2_32(sorter_type, sorter_ptr, 0, 3));
}

// Pull out the 'start_pos_' instance member from the provided Sorter instance
llvm::Value *Sorter::GetStartPosition(CodeGen &codegen,
                                      llvm::Value *sorter_ptr) const {
  auto *sorter_type = SorterProxy::GetType(codegen);
  return codegen->CreateLoad(
      codegen->CreateConstInBoundsGEP2_32(sorter_type, sorter_ptr, 0, 0));
}

llvm::Value *Sorter::GetTupleSize(CodeGen &codegen) const {
  return codegen.Const32(storage_format_.GetStorageSize());
}

//===----------------------------------------------------------------------===//
// SORTER ACCESS
//===----------------------------------------------------------------------===//

Sorter::SorterAccess::SorterAccess(const Sorter &sorter, llvm::Value *start_pos)
    : sorter_(sorter), start_pos_(start_pos) {}

Sorter::SorterAccess::Row &Sorter::SorterAccess::GetRow(llvm::Value *row_idx) {
  auto iter = cached_rows_.find(row_idx);
  if (iter == cached_rows_.end()) {
    cached_rows_.insert(std::make_pair(row_idx, Row{*this, row_idx}));
    iter = cached_rows_.find(row_idx);
  }
  return iter->second;
}

codegen::Value Sorter::SorterAccess::LoadRowValue(
    CodeGen &codegen, Sorter::SorterAccess::Row &row,
    uint32_t column_index) const {
  if (row.row_pos_ == nullptr) {
    auto *tuple_size = sorter_.GetTupleSize(codegen);
    auto *skip = codegen->CreateMul(row.row_idx_, tuple_size);
    row.row_pos_ =
        codegen->CreateInBoundsGEP(codegen.ByteType(), start_pos_, skip);
  }

  const auto &storage_format = sorter_.GetStorageFormat();
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_format,
                                            row.row_pos_};
  if (!null_bitmap.IsNullable(column_index)) {
    return storage_format.GetValueSkipNull(codegen, row.row_pos_, column_index);
  } else {
    return storage_format.GetValue(codegen, row.row_pos_, column_index,
                                   null_bitmap);
  }
}

//===----------------------------------------------------------------------===//
// SORTER ACCESS :: ROW
//===----------------------------------------------------------------------===//

Sorter::SorterAccess::Row::Row(SorterAccess &access, llvm::Value *row_index)
    : access_(access), row_idx_(row_index), row_pos_(nullptr) {}

codegen::Value Sorter::SorterAccess::Row::LoadColumn(CodeGen &codegen,
                                                     uint32_t column_index) {
  return access_.LoadRowValue(codegen, *this, column_index);
}

}  // namespace codegen
}  // namespace peloton