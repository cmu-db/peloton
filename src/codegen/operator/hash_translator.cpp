//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_translator.cpp
//
// Identification: src/codegen/operator/hash_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/hash_translator.h"

#include "planner/hash_plan.h"
#include "codegen/proxy/oa_hash_table_proxy.h"
#include "codegen/operator/projection_translator.h"
#include "codegen/lang/vectorized_loop.h"
#include "codegen/type/integer_type.h"
#include "common/logger.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// HASH TRANSLATOR
//===----------------------------------------------------------------------===//

// Constructor
HashTranslator::HashTranslator(const planner::HashPlan &hash_plan,
                               CompilationContext &context, Pipeline &pipeline)
    : OperatorTranslator(hash_plan, context, pipeline) {
  CodeGen &codegen = GetCodeGen();
  RuntimeState &runtime_state = context.GetRuntimeState();

  // Register the hash-table instance in the runtime state
  hash_table_id_ =
      runtime_state.RegisterState("hash", OAHashTableProxy::GetType(codegen));

  // Prepare the input operator
  context.Prepare(*hash_plan.GetChild(0), pipeline);

  // Prepare the hash keys
  std::vector<type::Type> key_type;
  const auto &hash_keys = hash_plan.GetHashKeys();
  for (const auto &hash_key : hash_keys) {
    // TODO: do the hash keys have to be prepared? Pipeline?
    context.Prepare(*hash_key);

    key_type.push_back(hash_key->ResultType());
  }

  // Create the hash table
  // we don't need to save any values, so value_size is zero
  hash_table_ = OAHashTable{codegen, key_type, 0};
}

// Initialize the hash table instance
void HashTranslator::InitializeQueryState() {
  hash_table_.Init(GetCodeGen(), LoadStatePtr(hash_table_id_));
}

// Produce!
void HashTranslator::Produce() const {
  // Let the child produce its tuples which we aggregate in our hash-table
  GetCompilationContext().Produce(*GetHashPlan().GetChild(0));
}

// Consume the tuples from the context, adding them to the hash table
void HashTranslator::Consume(ConsumerContext &context,
                             RowBatch::Row &row) const {
  CodeGen &codegen = GetCodeGen();

  // Collect the keys we use to probe the hash table
  std::vector<codegen::Value> key;
  CollectHashKeys(row, key);

  llvm::Value *hash = nullptr;

  // Perform the insertion into the hash table
  llvm::Value *hash_table = LoadStatePtr(hash_table_id_);

  ConsumerProbe probe{};
  ConsumerInsert insert{context, row};
  hash_table_.ProbeOrInsert(codegen, hash_table, hash, key, probe, insert);
}

// Cleanup by destroying the aggregation hash-table
void HashTranslator::TearDownQueryState() {
  hash_table_.Destroy(GetCodeGen(), LoadStatePtr(hash_table_id_));
}

void HashTranslator::CollectHashKeys(RowBatch::Row &row,
                                     std::vector<codegen::Value> &key) const {
  CodeGen &codegen = GetCodeGen();
  for (const auto &hash_key : GetHashPlan().GetHashKeys()) {
    key.push_back(row.DeriveValue(codegen, *hash_key));
  }
}

const planner::HashPlan &HashTranslator::GetHashPlan() const {
  return GetPlanAs<planner::HashPlan>();
}

//===----------------------------------------------------------------------===//
// CONSUMER PROBE
//===----------------------------------------------------------------------===//

// The callback invoked when we probe the hash table with a given key and find
// an existing entry for the key
void HashTranslator::ConsumerProbe::ProcessEntry(
    UNUSED_ATTRIBUTE CodeGen &codegen,
    UNUSED_ATTRIBUTE llvm::Value *data_area) const {
  // The key already exists in the hash table, which means that we can just drop
  // this row, as it already exists in the result
}

//===----------------------------------------------------------------------===//
// CONSUMER INSERT
//===----------------------------------------------------------------------===//

// Constructor
HashTranslator::ConsumerInsert::ConsumerInsert(ConsumerContext &context,
                                               RowBatch::Row &row)
    : context_(context), row_(row) {}

// Insert the key in the hash table on its first occurrence
void HashTranslator::ConsumerInsert::StoreValue(
    UNUSED_ATTRIBUTE CodeGen &codegen,
    UNUSED_ATTRIBUTE llvm::Value *space) const {
  // It is the first time this key appears, so we just pass it along pipeline
  context_.Consume(row_);
}

llvm::Value *HashTranslator::ConsumerInsert::GetValueSize(
    CodeGen &codegen) const {
  return codegen.Const32(0);
}

}  // namespace codegen
}  // namespace peloton