//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_translator.cpp
//
// Identification: src/codegen/operator/hash_join_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/hash_join_translator.h"

#include "codegen/expression/tuple_value_translator.h"
#include "codegen/lang/vectorized_loop.h"
#include "codegen/proxy/bloom_filter_proxy.h"
#include "codegen/proxy/oa_hash_table_proxy.h"
#include "expression/tuple_value_expression.h"
#include "planner/hash_join_plan.h"

namespace peloton {
namespace codegen {

std::atomic<bool> HashJoinTranslator::kUsePrefetch{false};

//===----------------------------------------------------------------------===//
// HASH JOIN TRANSLATOR
//===----------------------------------------------------------------------===//

// Constructor
HashJoinTranslator::HashJoinTranslator(const planner::HashJoinPlan &join,
                                       CompilationContext &context,
                                       Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), join_(join), left_pipeline_(this) {
  LOG_DEBUG("Constructing HashJoinTranslator ...");

  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();

  // If we should be prefetching into the hash-table, install a boundary in the
  // both the left and right pipeline at the input into this translator to
  // ensure it receives a vector of input tuples
  if (UsePrefetching()) {
    left_pipeline_.InstallBoundaryAtInput(this);
    pipeline.InstallBoundaryAtInput(this);
  }

  // Allocate state for our hash table and bloom filter
  hash_table_id_ =
      runtime_state.RegisterState("join", OAHashTableProxy::GetType(codegen));
  if (GetJoinPlan().IsBloomFilterEnabled()) {
    bloom_filter_id_ = runtime_state.RegisterState(
        "bloomfilter", BloomFilterProxy::GetType(codegen));
  }

  // Prepare translators for the left and right input operators
  context.Prepare(*join_.GetChild(0), left_pipeline_);
  context.Prepare(*join_.GetChild(1)->GetChild(0), pipeline);

  // Prepare the expressions that produce the build-size keys
  join.GetLeftHashKeys(left_key_exprs_);

  std::vector<type::Type> left_key_type;
  for (const auto *left_key : left_key_exprs_) {
    // Prepare the expression for translation
    context.Prepare(*left_key);

    // Collect the key types
    left_key_type.push_back(left_key->ResultType());
  }

  // Prepare the expressions that produce the probe-side keys
  join.GetRightHashKeys(right_key_exprs_);

  std::vector<type::Type> right_key_type;
  for (const auto *right_key : right_key_exprs_) {
    // Prepare the expression for translation
    context.Prepare(*right_key);

    // Collect the key types
    right_key_type.push_back(right_key->ResultType());
  }

  // Prepare the predicate
  auto *predicate = join_.GetPredicate();
  if (predicate != nullptr) {
    context.Prepare(*predicate);
  }

  // Make sure the key types are equal
  PL_ASSERT(left_key_type.size() == right_key_type.size());
  PL_ASSERT(std::equal(left_key_type.begin(), left_key_type.end(),
                       right_key_type.begin()));

  // Collect (unique) attributes that are stored in hash-table
  std::unordered_set<const planner::AttributeInfo *> left_key_ais;
  for (auto *left_key_exp : left_key_exprs_) {
    if (left_key_exp->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      auto *tve =
          static_cast<const expression::TupleValueExpression *>(left_key_exp);
      left_key_ais.insert(tve->GetAttributeRef());
    }
  }
  for (const auto *left_val_ai : join.GetLeftAttributes()) {
    if (left_key_ais.count(left_val_ai) == 0) {
      left_val_ais_.push_back(left_val_ai);
    }
  }

  // Construct the format of the left side
  std::vector<type::Type> left_value_types;
  for (const auto *left_val_ai : left_val_ais_) {
    left_value_types.push_back(left_val_ai->type);
  }
  left_value_storage_.Setup(codegen, left_value_types);

  // Check if the join needs an output vector to store saved probes
  if (pipeline.GetTranslatorStage(this) != 0) {
    // The join isn't the last operator in the pipeline, let's use a vector
    // TODO: In reality, we only need a vector if the attributes from the hash
    //       table are used in another stage. We're being lazy here ...
    needs_output_vector_ = false;
  }
  needs_output_vector_ = false;

  // Create the hash table
  hash_table_ =
      OAHashTable{codegen, left_key_type, left_value_storage_.MaxStorageSize()};
  LOG_DEBUG("Finished constructing HashJoinTranslator ...");
}

// Initialize the hash-table instance
void HashJoinTranslator::InitializeState() {
  hash_table_.Init(GetCodeGen(), LoadStatePtr(hash_table_id_));
  if (GetJoinPlan().IsBloomFilterEnabled()) {
    bloom_filter_.Init(GetCodeGen(), LoadStatePtr(bloom_filter_id_),
                       EstimateCardinalityLeft());
  }
}

// Produce!
void HashJoinTranslator::Produce() const {
  // Let the left child produce tuples which we materialize into the hash-table
  GetCompilationContext().Produce(*join_.GetChild(0));

  // Let the right child produce tuples, which we use to probe the hash table
  GetCompilationContext().Produce(*join_.GetChild(1)->GetChild(0));

  // That's it, we've produced all the tuples
}

void HashJoinTranslator::Consume(ConsumerContext &context,
                                 RowBatch &batch) const {
  if (!UsePrefetching()) {
    OperatorTranslator::Consume(context, batch);
    return;
  }

  // This aggregation uses prefetching
  // TODO: This code is copied verbatime from aggregation ... REFACTOR!

  auto &codegen = GetCodeGen();

  // The vector holding the hash values for the group
  auto *raw_vec = codegen.AllocateBuffer(
      codegen.Int64Type(), OAHashTable::kDefaultGroupPrefetchSize, "pfVector");
  Vector hashes{raw_vec, OAHashTable::kDefaultGroupPrefetchSize,
                codegen.Int64Type()};

  auto group_prefetch = [&](
      RowBatch::VectorizedIterateCallback::IterationInstance &iter_instance) {
    llvm::Value *p = codegen.Const32(0);
    llvm::Value *end =
        codegen->CreateSub(iter_instance.end, iter_instance.start);

    // The first loop does hash computation and prefetching
    lang::Loop prefetch_loop{
        codegen, codegen->CreateICmpULT(p, end), {{"p", p}}};
    {
      p = prefetch_loop.GetLoopVar(0);
      RowBatch::Row row =
          batch.GetRowAt(codegen->CreateAdd(p, iter_instance.start));

      // Collect keys
      std::vector<codegen::Value> key;
      if (IsFromLeftChild(context)) {
        CollectKeys(row, left_key_exprs_, key);
      } else {
        CollectKeys(row, right_key_exprs_, key);
      }

      // Hash the key and store in prefetch vector
      llvm::Value *hash_val = hash_table_.HashKey(codegen, key);

      // StoreValue hashed val in prefetch vector
      hashes.SetValue(codegen, p, hash_val);

      // Prefetch the actual hash table bucket
      hash_table_.PrefetchBucket(codegen, LoadStatePtr(hash_table_id_),
                                 hash_val, OAHashTable::PrefetchType::Read,
                                 OAHashTable::Locality::Medium);

      // End prefetch loop
      p = codegen->CreateAdd(p, codegen.Const32(1));
      prefetch_loop.LoopEnd(codegen->CreateICmpULT(p, end), {p});
    }

    p = codegen.Const32(0);
    std::vector<lang::Loop::LoopVariable> loop_vars = {
        {"p", p}, {"writeIdx", iter_instance.write_pos}};
    lang::Loop process_loop{codegen, codegen->CreateICmpULT(p, end), loop_vars};
    {
      p = process_loop.GetLoopVar(0);
      llvm::Value *write_pos = process_loop.GetLoopVar(1);

      llvm::Value *read_pos = codegen->CreateAdd(p, iter_instance.start);
      RowBatch::OutputTracker tracker{batch.GetSelectionVector(), write_pos};
      RowBatch::Row row = batch.GetRowAt(read_pos, &tracker);

      codegen::Value row_hash{type::Type{peloton::type::TypeId::INTEGER, false},
                              hashes.GetValue(codegen, p)};
      row.RegisterAttributeValue(&OAHashTable::kHashAI, row_hash);

      // Consume row
      Consume(context, row);

      // End prefetch loop
      p = codegen->CreateAdd(p, codegen.Const32(1));
      process_loop.LoopEnd(codegen->CreateICmpULT(p, end),
                           {p, tracker.GetFinalOutputPos()});
    }

    std::vector<llvm::Value *> final_vals;
    process_loop.CollectFinalLoopVariables(final_vals);

    return final_vals[0];
  };

  batch.VectorizedIterate(codegen, OAHashTable::kDefaultGroupPrefetchSize,
                          group_prefetch);
}

// Consume the tuples produced by a child operator
void HashJoinTranslator::Consume(ConsumerContext &context,
                                 RowBatch::Row &row) const {
  if (IsFromLeftChild(context)) {
    ConsumeFromLeft(context, row);
  } else {
    ConsumeFromRight(context, row);
  }
}

// The given row is coming from the left child. Insert into hash table
void HashJoinTranslator::ConsumeFromLeft(ConsumerContext &,
                                         RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();

  // Collect all the attributes we need for the join (including keys and vals)
  std::vector<codegen::Value> key;
  CollectKeys(row, left_key_exprs_, key);

  std::vector<codegen::Value> vals;
  CollectValues(row, left_val_ais_, vals);

  // If the hash value is available, use it
  llvm::Value *hash = nullptr;
  if (row.HasAttribute(&OAHashTable::kHashAI)) {
    codegen::Value hash_val = row.DeriveValue(codegen, &OAHashTable::kHashAI);
    hash = hash_val.GetValue();
  }

  // Insert tuples from the left side into the hash table
  InsertLeft insert_left{left_value_storage_, vals};
  hash_table_.Insert(codegen, LoadStatePtr(hash_table_id_), hash, key,
                     insert_left);

  if (GetJoinPlan().IsBloomFilterEnabled()) {
    // Insert tuples into the bloom filter if enabled
    bloom_filter_.Add(codegen, LoadStatePtr(bloom_filter_id_), key);
  }
}

// The given row is from the right child. Probe hash-table.
void HashJoinTranslator::ConsumeFromRight(ConsumerContext &context,
                                          RowBatch::Row &row) const {
  // Pull out the values of the keys we probe the hash-table with
  std::vector<codegen::Value> key;
  CollectKeys(row, right_key_exprs_, key);

  if (GetJoinPlan().IsBloomFilterEnabled()) {
    // Prefilter the tuple using Bloom Filter
    llvm::Value *contains = bloom_filter_.Contains(
        GetCodeGen(), LoadStatePtr(bloom_filter_id_), key);

    lang::If is_valid_row{GetCodeGen(), contains};
    {
      // For each tuple that passes the bloom filter, probe the hash table
      // to eliminate the false positives.
      CodegenHashProbe(context, row, key);
    }
    is_valid_row.EndIf();
  } else {
    // Bloom filter is not enabled. Directly probe the hash table
    CodegenHashProbe(context, row, key);
  }
}

void HashJoinTranslator::CodegenHashProbe(
    ConsumerContext &context, RowBatch::Row &row,
    std::vector<codegen::Value> &key) const {
  if (GetJoinPlan().GetJoinType() == JoinType::INNER) {
    // For inner joins, find all join partners
    ProbeRight probe_right{*this, context, row, key};
    hash_table_.FindAll(GetCodeGen(), LoadStatePtr(hash_table_id_), key,
                        probe_right);
  }
}

// Cleanup by destroying the hash-table instance
void HashJoinTranslator::TearDownState() {
  auto &codegen = GetCodeGen();
  hash_table_.Destroy(codegen, LoadStatePtr(hash_table_id_));
  if (GetJoinPlan().IsBloomFilterEnabled()) {
    bloom_filter_.Destroy(GetCodeGen(), LoadStatePtr(bloom_filter_id_));
  }
}

// Get the stringified name of this join
std::string HashJoinTranslator::GetName() const {
  std::string name = "HashJoin::";
  switch (join_.GetJoinType()) {
    case JoinType::INNER: {
      name.append("Inner");
      break;
    }
    case JoinType::OUTER: {
      name.append("Outer");
      break;
    }
    case JoinType::LEFT: {
      name.append("Left");
      break;
    }
    case JoinType::RIGHT: {
      name.append("Right");
      break;
    }
    case JoinType::SEMI: {
      name.append("Semi");
      break;
    }
    case JoinType::INVALID:
      throw Exception{"Invalid join type"};
  }
  return name;
}

// Estimate the size of the dynamically constructed hash-table
uint64_t HashJoinTranslator::EstimateHashTableSize() const {
  // TODO: Implement me
  return 0;
}

// Return the estimated number of tuples produced by the left child
uint64_t HashJoinTranslator::EstimateCardinalityLeft() const {
  // TODO:Implement this once optimizer provide cardinality to executor.
  // Right now, it's hard coded to a relatively large number to make sure
  // bloom filter works correctly.
  return (uint64_t)GetJoinPlan().GetChild(0)->GetCardinality();
}

// Should this aggregation use prefetching
bool HashJoinTranslator::UsePrefetching() const {
  // TODO: Implement me
  return kUsePrefetch;
}

void HashJoinTranslator::CollectKeys(
    RowBatch::Row &row,
    const std::vector<const expression::AbstractExpression *> &key,
    std::vector<codegen::Value> &key_values) const {
  for (const auto *exp : key) {
    key_values.push_back(row.DeriveValue(GetCodeGen(), *exp));
  }
}

void HashJoinTranslator::CollectValues(
    RowBatch::Row &row, const std::vector<const planner::AttributeInfo *> &ais,
    std::vector<codegen::Value> &values) const {
  for (const auto *ai : ais) {
    values.push_back(row.DeriveValue(GetCodeGen(), ai));
  }
}

//===----------------------------------------------------------------------===//
// PROBE RIGHT
//===----------------------------------------------------------------------===//

HashJoinTranslator::ProbeRight::ProbeRight(
    const HashJoinTranslator &join_translator, ConsumerContext &context,
    RowBatch::Row &row, const std::vector<codegen::Value> &right_key)
    : join_translator_(join_translator),
      context_(context),
      row_(row),
      right_key_(right_key) {}

// The callback invoked when iterating the hash table.  The key and value of
// the current hash table entry are provided as parameters.  We add these to
// the context and pass it up the tree.
void HashJoinTranslator::ProbeRight::ProcessEntry(
    CodeGen &codegen, const std::vector<codegen::Value> &key,
    llvm::Value *data_area) const {
  const auto &storage = join_translator_.left_value_storage_;

  if (join_translator_.needs_output_vector_) {
    // Use output vector for attribute access
    throw Exception{"Shouldn't need output"};
  } else {
    // LoadValues all the values from the hash entry
    std::vector<codegen::Value> left_vals;
    storage.LoadValues(codegen, data_area, left_vals);

    // Put the values directly into the row
    const auto &left_val_ais = join_translator_.left_val_ais_;
    for (uint32_t i = 0; i < left_val_ais.size(); i++) {
      row_.RegisterAttributeValue(left_val_ais[i], left_vals[i]);
    }

    const auto &left_key_exprs = join_translator_.left_key_exprs_;
    for (uint32_t i = 0; i < left_key_exprs.size(); i++) {
      const auto *exp = left_key_exprs[i];
      if (exp->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
        auto *tve = static_cast<const expression::TupleValueExpression *>(exp);
        codegen::Value v = key[i];
        LOG_DEBUG("Putting AI %s (%p) into row",
                  tve->GetAttributeRef()->name.c_str(), tve->GetAttributeRef());
        row_.RegisterAttributeValue(tve->GetAttributeRef(), v);
      }
    }
  }

  // Check predicate if one exists
  auto *predicate = join_translator_.GetJoinPlan().GetPredicate();
  if (predicate != nullptr) {
    // Vectorize of TaaT filter?
    auto valid_row = row_.DeriveValue(codegen, *predicate);
    lang::If is_valid_row{codegen, valid_row};
    {
      // Send row up to the parent
      context_.Consume(row_);
    }
    is_valid_row.EndIf();
  } else {
    // Send the row up to the parent
    context_.Consume(row_);
  }
}

//===----------------------------------------------------------------------===//
// INSERT LEFT
//===----------------------------------------------------------------------===//

HashJoinTranslator::InsertLeft::InsertLeft(
    const CompactStorage &storage, const std::vector<codegen::Value> &values)
    : storage_(storage), values_(values) {}

// Store the attributes from the left-side input into the provided storage space
void HashJoinTranslator::InsertLeft::StoreValue(CodeGen &codegen,
                                                llvm::Value *space) const {
  storage_.StoreValues(codegen, space, values_);
}

llvm::Value *HashJoinTranslator::InsertLeft::GetValueSize(
    CodeGen &codegen) const {
  return codegen.Const32(storage_.MaxStorageSize());
}

}  // namespace codegen
}  // namespace peloton