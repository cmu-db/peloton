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
    : OperatorTranslator(context, pipeline), hash_plan_(hash_plan) {
  LOG_DEBUG("Constructing HashTranslator ...");

  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();

  // Register the hash-table instance in the runtime state
  hash_table_id_ =
      runtime_state.RegisterState("hash", OAHashTableProxy::GetType(codegen));

  // Prepare the input operator
  context.Prepare(*hash_plan_.GetChild(0), pipeline);

  // Prepare the hash keys
  std::vector<type::Type> key_type;
  const auto &hash_keys = hash_plan_.GetHashKeys();
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
void HashTranslator::InitializeState() {
  hash_table_.Init(GetCodeGen(), LoadStatePtr(hash_table_id_));
}

// Produce!
std::vector<CodeGenStage> HashTranslator::Produce() const {
  auto &compilation_context = GetCompilationContext();

  LOG_DEBUG("Hash starting to produce results ...");

  // Let the child produce its tuples which we aggregate in our hash-table
  return compilation_context.Produce(*hash_plan_.GetChild(0));
}

// Consume the tuples from the context, adding them to the hash table
void HashTranslator::Consume(ConsumerContext &context,
                             RowBatch::Row &row) const {
  LOG_DEBUG("Hash operator consuming results ...");

  auto &codegen = GetCodeGen();

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
void HashTranslator::TearDownState() {
  hash_table_.Destroy(GetCodeGen(), LoadStatePtr(hash_table_id_));
}

// Get the stringified name of this hash operator
std::string HashTranslator::GetName() const { return "Hash"; }

// Estimate the size of the dynamically constructed hash-table
uint64_t HashTranslator::EstimateHashTableSize() const {
  // TODO: Implement me
  return 0;
}

void HashTranslator::CollectHashKeys(RowBatch::Row &row,
                                     std::vector<codegen::Value> &key) const {
  auto &codegen = GetCodeGen();
  for (const auto &hash_key : hash_plan_.GetHashKeys()) {
    key.push_back(row.DeriveValue(codegen, *hash_key));
  }
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
  // this row,
  // as it already exists in the result
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
  // It is the first time this key appears, so we just pass it on to the next
  // operator

  // call consume on parent
  context_.Consume(row_);
}

llvm::Value *HashTranslator::ConsumerInsert::GetValueSize(
    CodeGen &codegen) const {
  return codegen.Const32(0);
}

}  // namespace codegen
}  // namespace peloton