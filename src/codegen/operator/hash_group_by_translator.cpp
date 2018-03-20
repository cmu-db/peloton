//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.cpp
//
// Identification: src/codegen/operator/hash_group_by_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/hash_group_by_translator.h"

#include "codegen/compilation_context.h"
#include "codegen/proxy/oa_hash_table_proxy.h"
#include "codegen/operator/projection_translator.h"
#include "codegen/lang/vectorized_loop.h"
#include "codegen/type/integer_type.h"
#include "common/logger.h"

namespace peloton {
namespace codegen {

std::atomic<bool> HashGroupByTranslator::kUsePrefetch{false};

//===----------------------------------------------------------------------===//
// HASH GROUP BY TRANSLATOR
//===----------------------------------------------------------------------===//

// Constructor
HashGroupByTranslator::HashGroupByTranslator(
    const planner::AggregatePlan &group_by, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      group_by_(group_by),
      child_pipeline_(this),
      aggregation_(context.GetRuntimeState()) {
  LOG_DEBUG("Constructing HashGroupByTranslator ...");

  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();

  // If we should be prefetching into the hash-table, install a boundary in the
  // pipeline at the input into this translator to ensure it receives a vector
  // of input tuples
  if (UsePrefetching()) {
    child_pipeline_.InstallBoundaryAtInput(this);
  }

  // Register the hash-table instance in the runtime state
  hash_table_id_ = runtime_state.RegisterState(
      "groupBy", OAHashTableProxy::GetType(codegen));

  // Prepare the input operator to this group by
  context.Prepare(*group_by_.GetChild(0), child_pipeline_);

  // Prepare the predicate if one exists
  if (group_by_.GetPredicate() != nullptr) {
    context.Prepare(*group_by_.GetPredicate());
  }

  // Prepare the grouping expressions
  // TODO: We need to handle grouping keys that are expressions (i.e., prepare)
  std::vector<type::Type> key_type;
  const auto &grouping_ais = group_by_.GetGroupbyAIs();
  for (const auto *grouping_ai : grouping_ais) {
    key_type.push_back(grouping_ai->type);
  }

  // Prepare all the aggregation expressions and setup the storage format of
  // values/aggregates in the hash table
  auto &aggregates = group_by_.GetUniqueAggTerms();
  for (const auto &agg_term : aggregates) {
    if (agg_term.expression != nullptr) {
      context.Prepare(*agg_term.expression);
    }
  }

  // Prepare the projection (if one exists)
  const auto *projection_info = group_by_.GetProjectInfo();
  if (projection_info != nullptr) {
    ProjectionTranslator::PrepareProjection(context, *projection_info);
  }

  // Setup the aggregation logic for this group by
  aggregation_.Setup(codegen, aggregates, false, key_type);

  // Create the hash table
  hash_table_ =
      OAHashTable{codegen, key_type, aggregation_.GetAggregatesStorageSize()};
}

// Initialize the hash table instance
void HashGroupByTranslator::InitializeState() {
  hash_table_.Init(GetCodeGen(), LoadStatePtr(hash_table_id_));
  aggregation_.InitializeState(GetCodeGen());
}

// Produce!
void HashGroupByTranslator::Produce() const {
  auto &comp_ctx = GetCompilationContext();

  // Let the child produce its tuples which we aggregate in our hash-table
  comp_ctx.Produce(*group_by_.GetChild(0));

  LOG_DEBUG("HashGroupBy starting to produce results ...");

  auto &codegen = GetCodeGen();

  // Iterate over the hash table, sending tuples up the tree
  auto *raw_vec = codegen.AllocateBuffer(
      codegen.Int32Type(), Vector::kDefaultVectorSize, "hashGroupBySelVector");
  Vector selection_vec{raw_vec, Vector::kDefaultVectorSize,
                       GetCodeGen().Int32Type()};
  ProduceResults producer{*this};
  hash_table_.VectorizedIterate(GetCodeGen(), LoadStatePtr(hash_table_id_),
                                selection_vec, producer);
}

void HashGroupByTranslator::Consume(ConsumerContext &context,
                                    RowBatch &batch) const {
  if (!UsePrefetching()) {
    OperatorTranslator::Consume(context, batch);
    return;
  }

  // This aggregation uses prefetching

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
      CollectHashKeys(row, key);

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

      codegen::Value row_hash{type::Integer::Instance(),
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

// Consume the tuples from the context, grouping them into the hash table
void HashGroupByTranslator::Consume(ConsumerContext &,
                                    RowBatch::Row &row) const {
  LOG_DEBUG("HashGroupBy consuming results ...");

  auto &context = GetCompilationContext();
  auto &codegen = GetCodeGen();

  // Collect the keys we use to probe the hash table
  std::vector<codegen::Value> key;
  CollectHashKeys(row, key);

  // Collect the values of the expressions
  auto &aggregates = group_by_.GetUniqueAggTerms();
  std::vector<codegen::Value> vals{aggregates.size()};
  for (uint32_t i = 0; i < aggregates.size(); i++) {
    const auto &agg_term = aggregates[i];
    if (agg_term.expression != nullptr) {
      vals[i] = row.DeriveValue(codegen, *agg_term.expression);
    }
  }

  // If the hash value is available, use it
  llvm::Value *hash = nullptr;
  if (row.HasAttribute(&OAHashTable::kHashAI)) {
    codegen::Value hash_val = row.DeriveValue(codegen, &OAHashTable::kHashAI);
    hash = hash_val.GetValue();
  }

  // Perform the insertion into the hash table
  llvm::Value *hash_table = LoadStatePtr(hash_table_id_);
  ConsumerProbe probe{context, aggregation_, vals, key};
  ConsumerInsert insert{aggregation_, vals, key};
  hash_table_.ProbeOrInsert(codegen, hash_table, hash, key, probe, insert);
}

// Cleanup by destroying the aggregation hash-table
void HashGroupByTranslator::TearDownState() {
  hash_table_.Destroy(GetCodeGen(), LoadStatePtr(hash_table_id_));
  aggregation_.TearDownState(GetCodeGen());
}

// Get the stringified name of this hash-based group-by
std::string HashGroupByTranslator::GetName() const { return "HashGroupBy"; }

// Estimate the size of the dynamically constructed hash-table
uint64_t HashGroupByTranslator::EstimateHashTableSize() const {
  // TODO: Implement me
  return 0;
}

// Should this aggregation use prefetching
bool HashGroupByTranslator::UsePrefetching() const {
  // TODO: Implement me
  return kUsePrefetch;
}

void HashGroupByTranslator::CollectHashKeys(
    RowBatch::Row &row, std::vector<codegen::Value> &key) const {
  auto &codegen = GetCodeGen();
  for (const auto *gb_ai : group_by_.GetGroupbyAIs()) {
    key.push_back(row.DeriveValue(codegen, gb_ai));
  }
}

//===----------------------------------------------------------------------===//
// AGGREGATE FINALIZER
//===----------------------------------------------------------------------===//

HashGroupByTranslator::AggregateFinalizer::AggregateFinalizer(
    const Aggregation &aggregation,
    HashTable::HashTableAccess &hash_table_access)
    : aggregation_(aggregation),
      hash_table_access_(hash_table_access),
      finalized_(false) {}

const std::vector<codegen::Value> &
HashGroupByTranslator::AggregateFinalizer::GetAggregates(CodeGen &codegen,
                                                         llvm::Value *index) {
  if (!finalized_) {
    // It hasn't been finalized yet, do so now

    // First extract keys from buckets
    hash_table_access_.ExtractBucketKeys(codegen, index, final_aggregates_);

    // Now extract aggregate values
    llvm::Value *data_area = hash_table_access_.BucketValue(codegen, index);
    aggregation_.FinalizeValues(codegen, data_area, final_aggregates_);
    finalized_ = true;
  }
  return final_aggregates_;
}

//===----------------------------------------------------------------------===//
// AGGREGATE ACCESS
//===----------------------------------------------------------------------===//

HashGroupByTranslator::AggregateAccess::AggregateAccess(
    AggregateFinalizer &finalizer, uint32_t agg_index)
    : finalizer_(finalizer), agg_index_(agg_index) {}

codegen::Value HashGroupByTranslator::AggregateAccess::Access(
    CodeGen &codegen, RowBatch::Row &row) {
  auto *pos = row.GetTID(codegen);
  const auto &final_agg_vals = finalizer_.GetAggregates(codegen, pos);
  return final_agg_vals[agg_index_];
}

//===----------------------------------------------------------------------===//
// PRODUCE RESULTS
//===----------------------------------------------------------------------===//

HashGroupByTranslator::ProduceResults::ProduceResults(
    const HashGroupByTranslator &translator)
    : translator_(translator) {}

void HashGroupByTranslator::ProduceResults::ProcessEntries(
    CodeGen &, llvm::Value *start, llvm::Value *end, Vector &selection_vector,
    HashTable::HashTableAccess &access) const {
  RowBatch batch{translator_.GetCompilationContext(), start, end,
                 selection_vector, true};

  AggregateFinalizer finalizer{translator_.GetAggregation(), access};

  const auto &group_by = translator_.GetAggregatePlan();
  auto &grouping_ais = group_by.GetGroupbyAIs();
  auto &aggregates = group_by.GetUniqueAggTerms();

  std::vector<AggregateAccess> accessors;

  // Add accessors for each grouping key and aggregate value
  for (uint64_t i = 0; i < grouping_ais.size() + aggregates.size(); i++) {
    accessors.emplace_back(finalizer, i);
  }

  // Register attributes in the row batch
  for (uint64_t i = 0; i < grouping_ais.size(); i++) {
    LOG_DEBUG("Adding aggregate key attribute '%s' (%p) to batch",
              grouping_ais[i]->name.c_str(), grouping_ais[i]);
    batch.AddAttribute(grouping_ais[i], &accessors[i]);
  }
  for (uint64_t i = 0; i < aggregates.size(); i++) {
    auto &agg_term = aggregates[i];
    LOG_DEBUG("Adding aggregate attribute '%s' (%p) to batch",
              agg_term.agg_ai.name.c_str(), &agg_term.agg_ai);
    batch.AddAttribute(&agg_term.agg_ai, &accessors[i + grouping_ais.size()]);
  }

  std::vector<RowBatch::ExpressionAccess> derived_attribute_accessors;
  const auto *project_info = group_by.GetProjectInfo();
  if (project_info != nullptr) {
    ProjectionTranslator::AddNonTrivialAttributes(batch, *project_info,
                                                  derived_attribute_accessors);
  }

  // Row batch is set up, send it up
  ConsumerContext context{translator_.GetCompilationContext(),
                          translator_.GetPipeline()};

  auto *predicate = group_by.GetPredicate();
  if (predicate != nullptr) {
    // There is a predicate that must be checked
    auto &codegen = translator_.GetCodeGen();

    // Iterate over the batch, performing a branching predicate check
    batch.Iterate(codegen, [&](RowBatch::Row &row) {
      codegen::Value valid_row = row.DeriveValue(codegen, *predicate);
      lang::If is_valid_row{codegen, valid_row};
      {
        // The row is valid, send along the pipeline
        context.Consume(row);
      }
      is_valid_row.EndIf();
    });

  } else {
    // There isn't a predicate, just send the entire batch as-is
    context.Consume(batch);
  }
}

//===----------------------------------------------------------------------===//
// CONSUMER PROBE
//===----------------------------------------------------------------------===//

// Constructor
HashGroupByTranslator::ConsumerProbe::ConsumerProbe(
    CompilationContext &context, const Aggregation &aggregation,
    const std::vector<codegen::Value> &next_vals,
    const std::vector<codegen::Value> &grouping_keys)
    : context_(context),
      aggregation_(aggregation),
      next_vals_(next_vals),
      grouping_keys_(grouping_keys) {}

// The callback invoked when we probe the hash table with a given key and find
// an existing value for the key.  In this case, since we're aggregating, we
// advance all of the aggregates.
void HashGroupByTranslator::ConsumerProbe::ProcessEntry(
    UNUSED_ATTRIBUTE CodeGen &codegen, llvm::Value *data_area) const {
  aggregation_.AdvanceValues(codegen, data_area, next_vals_, grouping_keys_);
}

//===----------------------------------------------------------------------===//
// CONSUMER INSERT
//===----------------------------------------------------------------------===//

HashGroupByTranslator::ConsumerInsert::ConsumerInsert(
    const Aggregation &aggregation,
    const std::vector<codegen::Value> &initial_vals,
    const std::vector<codegen::Value> &grouping_keys)
    : aggregation_(aggregation),
      initial_vals_(initial_vals),
      grouping_keys_(grouping_keys) {}

// Given free storage space in the hash table, store the initial values of all
// the aggregates
void HashGroupByTranslator::ConsumerInsert::StoreValue(
    UNUSED_ATTRIBUTE CodeGen &codegen, llvm::Value *space) const {
  aggregation_.CreateInitialValues(codegen, space, initial_vals_,
                                   grouping_keys_);
}

llvm::Value *HashGroupByTranslator::ConsumerInsert::GetValueSize(
    CodeGen &codegen) const {
  return codegen.Const32(aggregation_.GetAggregatesStorageSize());
}

}  // namespace codegen
}  // namespace peloton