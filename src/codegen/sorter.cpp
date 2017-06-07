//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter.cpp
//
// Identification: src/codegen/sorter.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/sorter.h"

#include "include/codegen/util/loop.h"
#include "include/codegen/proxy/sorter_proxy.h"
#include "codegen/vectorized_loop.h"
#include "codegen/vector.h"

namespace peloton {
namespace codegen {

Sorter::Sorter() {
  // This constructor shouldn't generally be used at all, but there are
  // cases when the tuple description is not known fully at construction time.
}

Sorter::Sorter(CodeGen &codegen,
               const std::vector<type::Type::TypeId> &row_desc) {
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
  codegen.CallFunc(SorterProxy::_Init::GetFunction(codegen),
                   {sorter_ptr, comparison_func, tuple_size});
}

// Append the given tuple into the sorter instance
void Sorter::Append(CodeGen &codegen, llvm::Value *sorter_ptr,
                    const std::vector<codegen::Value> &tuple) const {
  // First, call Sorter::StoreInputTuple() to get a handle to a contiguous
  // chunk of free space large enough to materialize a single tuple.
  auto *store_func = SorterProxy::_StoreInputTuple::GetFunction(codegen);
  auto *space = codegen.CallFunc(store_func, {sorter_ptr});

  // Now, individually store the attributes of the tuple into the free space
  for (uint32_t col_id = 0; col_id < tuple.size(); col_id++) {
    storage_format_.SetValueAt(codegen, space, col_id, tuple[col_id]);
  }
}

// Just make a call to util::Sorter::Sort(...). This actually sorts the data
// that has been inserted into the sorter instance.
void Sorter::Sort(CodeGen &codegen, llvm::Value *sorter_ptr) const {
  auto *sort_func = SorterProxy::_Sort::GetFunction(codegen);
  codegen.CallFunc(sort_func, {sorter_ptr});
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
      Loop loop{codegen,
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

  // Determine the number of bytes to skip per vector
  llvm::Value *vec_sz = codegen.Const32(vector_size);
  llvm::Value *tuple_size = GetTupleSize(codegen);
  llvm::Value *skip = codegen->CreateMul(vec_sz, tuple_size);

  VectorizedLoop loop{codegen, num_tuples, vector_size, {{"pos", start_pos}}};
  {
    llvm::Value *curr_pos = loop.GetLoopVar(0);
    auto curr_range = loop.GetCurrentRange();

    // Provide an accessor into the sorted space
    SorterAccess sorter_access{*this, start_pos};

    // Issue the callback
    callback.ProcessEntries(codegen, curr_range.start, curr_range.end,
                            sorter_access);

    // Bump the pointer by the size of a tuple
    llvm::Value *next_pos = codegen->CreateInBoundsGEP(curr_pos, skip);
    loop.LoopEnd(codegen, {next_pos});
  }
}

// Just make a call to util::Sorter::Destroy(...)
void Sorter::Destroy(CodeGen &codegen, llvm::Value *sorter_ptr) const {
  codegen.CallFunc(SorterProxy::_Destroy::GetFunction(codegen), {sorter_ptr});
}

llvm::Value *Sorter::GetNumberOfStoredTuples(CodeGen &codegen,
                                             llvm::Value *sorter_ptr) const {
  // TODO: util::Sorter has a function to handle this ...
  llvm::Value *start_pos = GetStartPosition(codegen, sorter_ptr);
  llvm::Value *end_pos = GetEndPosition(codegen, sorter_ptr);
  llvm::Value *tuple_size =
      codegen->CreateZExt(GetTupleSize(codegen), codegen.Int64Type());

  llvm::Value *diff_bytes = codegen->CreatePtrDiff(end_pos, start_pos);
  llvm::Value *num_tuples = codegen->CreateUDiv(diff_bytes, tuple_size);
  return codegen->CreateTruncOrBitCast(num_tuples, codegen.Int32Type());
}

// Pull out the 'start_pos_' instance member from the provided Sorter instance
llvm::Value *Sorter::GetStartPosition(CodeGen &codegen,
                                      llvm::Value *sorter_ptr) const {
  auto *sorter_type = SorterProxy::GetType(codegen);
  return codegen->CreateLoad(
      codegen->CreateConstInBoundsGEP2_32(sorter_type, sorter_ptr, 0, 0));
}

// Pull out the 'end_pos_' instance member from the provided Sorter instance
llvm::Value *Sorter::GetEndPosition(CodeGen &codegen,
                                    llvm::Value *sorter_ptr) const {
  auto *sorter_type = SorterProxy::GetType(codegen);
  return codegen->CreateLoad(
      codegen->CreateConstInBoundsGEP2_32(sorter_type, sorter_ptr, 0, 1));
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
  return storage_format.GetValueAt(codegen, row.row_pos_, column_index);
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