//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregation.cpp
//
// Identification: src/codegen/aggregation.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/aggregation.h"

#include "codegen/lang/if.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/bigint_type.h"
#include "codegen/type/decimal_type.h"
#include "codegen/proxy/oa_hash_table_proxy.h"
#include "common/logger.h"

namespace peloton {
namespace codegen {

// TODO: Handle shared/duplicated aggregates and hash table sharing

// Configure/setup the aggregation class to handle the provided aggregate types
void Aggregation::Setup(
    CompilationContext &context,
    const std::vector<planner::AggregatePlan::AggTerm> &aggregates,
    bool is_global, std::vector<type::Type> &grouping_ai_types) {
  auto &codegen = context.GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();

  is_global_ = is_global;

  for (uint32_t source_idx = 0; source_idx < aggregates.size(); source_idx++) {
    const auto &agg_term = aggregates[source_idx];
    switch (agg_term.aggtype) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Add the count to the storage layout. COUNTs are never NULL-able.
        const type::Type count_type{type::BigInt::Instance()};
        uint32_t storage_pos = storage_.AddType(count_type);

        // add hash table if distinct
        if (agg_term.distinct) {
          // Register the hash-table instance in the runtime state
          auto hash_table_id = runtime_state.RegisterState(
              "agg_hash" + std::to_string(source_idx),
              OAHashTableProxy::GetType(codegen));

          // Prepare the hash keys
          // if not global, start with grouping keys, then add aggregation key
          std::vector<type::Type> key_type;
          if (!IsGlobal()) {
            key_type = grouping_ai_types;
          }
          key_type.push_back(agg_term.expression->ResultType());

          // Create the hash table
          // we don't need to save any values, so value_size is zero
          auto hash_table = OAHashTable{codegen, key_type, 0};

          hash_table_infos_.emplace_back(
              std::pair<OAHashTable, RuntimeState::StateID>(
                  std::move(hash_table), hash_table_id));
        }

        // Add metadata for the count aggregate
        // If aggregate is not distinct, it doesn't matter if the
        // hash_table_index is an invalid value
        AggregateInfo aggregate_info{
            agg_term.aggtype, count_type, source_idx, storage_pos, false,
            agg_term.distinct,
            static_cast<uint32_t>(hash_table_infos_.size() - 1)};
        aggregate_infos_.push_back(aggregate_info);
        break;
      }
      case ExpressionType::AGGREGATE_SUM: {
        // add hash table if distinct
        if (agg_term.distinct) {
          // Register the hash-table instance in the runtime state
          auto hash_table_id = runtime_state.RegisterState(
              "agg_hash" + std::to_string(source_idx),
              OAHashTableProxy::GetType(codegen));

          // Prepare the hash keys
          // if not global, start with grouping keys, then add aggregation key
          std::vector<type::Type> key_type;
          if (!IsGlobal()) {
            key_type = grouping_ai_types;
          }
          key_type.push_back(agg_term.expression->ResultType());

          // Create the hash table
          // we don't need to save any values, so value_size is zero
          auto hash_table = OAHashTable{codegen, key_type, 0};

          hash_table_infos_.emplace_back(
              std::pair<OAHashTable, RuntimeState::StateID>(
                  std::move(hash_table), hash_table_id));
        }

        // Add the element to the storage layout
        auto value_type = agg_term.expression->ResultType();

        // If we're doing a global aggregation, the aggregate can potentially be
        // NULL (i.e., if there are no rows in the source table).
        if (IsGlobal()) {
          value_type = value_type.AsNullable();
        }
        uint32_t storage_pos = storage_.AddType(value_type);

        // Add metadata for the aggregate
        AggregateInfo aggregate_info{
            agg_term.aggtype, value_type, source_idx, storage_pos, false,
            agg_term.distinct,
            static_cast<uint32_t>(hash_table_infos_.size() - 1)};
        aggregate_infos_.push_back(aggregate_info);
        break;
      }
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        // It doesn't pay off for MIN/MAX to use a hash table for distinct
        // values,
        // since applying the MIM/MAX instruction every time is cheaper - so
        // skip the distinct part

        // Add the element to the storage layout
        auto value_type = agg_term.expression->ResultType();

        // If we're doing a global aggregation, the aggregate can potentially be
        // NULL (i.e., if there are no rows in the source table).
        if (IsGlobal()) {
          value_type = value_type.AsNullable();
        }
        uint32_t storage_pos = storage_.AddType(value_type);

        // Add metadata for the aggregate
        AggregateInfo aggregate_info{
            agg_term.aggtype, value_type, source_idx, storage_pos, false,
            false,  // always set false, no distinct here
            // TODO: when merging aggregations, it doesn't matter if MIN/MAX is
            // distinct or not
            std::numeric_limits<uint32_t>::max()};
        aggregate_infos_.push_back(aggregate_info);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // We decompose averages into separate COUNT() and SUM() components

        unsigned long sum_hash_table_info_index = 0;
        unsigned long count_hash_table_info_index = 0;
        // add hash tables if distinct
        if (agg_term.distinct) {
          // Register the hash-table instance in the runtime state
          auto sum_hash_table_id = runtime_state.RegisterState(
              "agg_hash" + std::to_string(source_idx) + "_sum",
              OAHashTableProxy::GetType(codegen));
          auto count_hash_table_id = runtime_state.RegisterState(
              "agg_hash" + std::to_string(source_idx) + "_count",
              OAHashTableProxy::GetType(codegen));

          // Prepare the hash keys
          // if not global, start with grouping keys, then add aggregation key
          std::vector<type::Type> key_type;
          if (!IsGlobal()) {
            key_type = grouping_ai_types;
          }
          key_type.push_back(agg_term.expression->ResultType());

          // Create the hash table
          // we don't need to save any values, so value_size is zero
          auto sum_hash_table = OAHashTable{codegen, key_type, 0};
          auto count_hash_table = OAHashTable{codegen, key_type, 0};

          hash_table_infos_.emplace_back(
              std::pair<OAHashTable, RuntimeState::StateID>(
                  std::move(sum_hash_table), sum_hash_table_id));
          sum_hash_table_info_index = hash_table_infos_.size() - 1;
          hash_table_infos_.emplace_back(
              std::pair<OAHashTable, RuntimeState::StateID>(
                  std::move(count_hash_table), count_hash_table_id));
          count_hash_table_info_index = hash_table_infos_.size() - 1;
        }

        // SUM() - the type must match the type of the expression
        // First add the sum to the storage layout
        PL_ASSERT(agg_term.expression != nullptr);
        auto sum_type = agg_term.expression->ResultType();
        if (IsGlobal()) {
          sum_type = sum_type.AsNullable();
        }
        uint32_t sum_storage_pos = storage_.AddType(sum_type);

        // Add metadata about the SUM() aggregate
        AggregateInfo sum_agg{ExpressionType::AGGREGATE_SUM, sum_type,
                              source_idx, sum_storage_pos, true,
                              agg_term.distinct,
                              static_cast<uint32_t>(sum_hash_table_info_index)};
        aggregate_infos_.push_back(sum_agg);

        // COUNT() - can use big integer since we're counting instances
        // First add the count to the storage layout
        uint32_t count_storage_pos = storage_.AddType(type::BigInt::Instance());

        // Add metadata about the COUNT() aggregate
        AggregateInfo count_agg{
            ExpressionType::AGGREGATE_COUNT, type::BigInt::Instance(),
            source_idx, count_storage_pos, true, agg_term.distinct,
            static_cast<uint32_t>(count_hash_table_info_index)};
        aggregate_infos_.push_back(count_agg);

        // AVG() - this isn't storage physically, but we need metadata about it
        AggregateInfo avg_agg{agg_term.aggtype, type::Decimal::Instance(),
                              source_idx, source_idx, false, agg_term.distinct,
                              std::numeric_limits<uint32_t>::max()};
        aggregate_infos_.push_back(avg_agg);
        break;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when preparing aggregator",
            ExpressionTypeToString(agg_term.aggtype).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, message};
      }
    }
  }

  // Finalize the storage format
  storage_.Finalize(codegen);
}

// Setup the aggregation to handle the provided aggregates
void Aggregation::Setup(
    CompilationContext &context,
    const std::vector<planner::AggregatePlan::AggTerm> &agg_terms,
    bool is_global) {
  PL_ASSERT(is_global);
  // Create empty vector and hand the reference to the actual implementation
  // makes it easier to call this function without providing grouping keys
  std::vector<type::Type> empty;
  Setup(context, agg_terms, is_global, empty);
}

// Codegen any initialization work for the hash tables
void Aggregation::InitializeState(CompilationContext &context) {
  auto &codegen = context.GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();

  for (auto hash_table_info : hash_table_infos_) {
    auto &hash_table = hash_table_info.first;
    auto hash_table_id = hash_table_info.second;
    hash_table.Init(codegen,
                    runtime_state.LoadStatePtr(codegen, hash_table_id));
  }
}

// Cleanup by destroying the aggregation hash tables
void Aggregation::TearDownState(CompilationContext &context) {
  auto &codegen = context.GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();

  for (auto hash_table_info : hash_table_infos_) {
    auto &hash_table = hash_table_info.first;
    auto hash_table_id = hash_table_info.second;
    hash_table.Destroy(codegen,
                       runtime_state.LoadStatePtr(codegen, hash_table_id));
  }
}

void Aggregation::CreateInitialGlobalValues(CodeGen &codegen,
                                            llvm::Value *space) const {
  PL_ASSERT(IsGlobal());
  auto null_bitmap =
      UpdateableStorage::NullBitmap{codegen, GetAggregateStorage(), space};
  null_bitmap.InitAllNull(codegen);
  null_bitmap.WriteBack(codegen);
}

// Create the initial values of all aggregates based on the the provided values
void Aggregation::CreateInitialValues(
    CompilationContext &context, llvm::Value *space,
    const std::vector<codegen::Value> &initial,
    const std::vector<codegen::Value> &grouping_keys) const {
  // Global aggregations should be calling CreateInitialGlobalValues(...)
  PL_ASSERT(!IsGlobal());

  auto &codegen = context.GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();

  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, space};

  // Initialize bitmap to all NULLs
  null_bitmap.InitAllNull(codegen);

  for (uint32_t i = 0; i < aggregate_infos_.size(); i++) {
    const auto &agg_info = aggregate_infos_[i];
    const auto &input_val = initial[agg_info.source_index];

    switch (agg_info.aggregate_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        // For the above aggregations, the initial value is the attribute value
        if (null_bitmap.IsNullable(agg_info.storage_index)) {
          storage_.SetValue(codegen, space, agg_info.storage_index, input_val,
                            null_bitmap);
        } else {
          storage_.SetValueSkipNull(codegen, space, agg_info.storage_index,
                                    input_val);
        }
        break;
      }
      case ExpressionType::AGGREGATE_COUNT: {
        llvm::Value *raw_initial = nullptr;
        if (input_val.IsNullable()) {
          llvm::Value *not_null = input_val.IsNotNull(codegen);
          raw_initial = codegen->CreateZExt(not_null, codegen.Int64Type());
        } else {
          raw_initial = codegen.Const64(1);
        }
        auto initial_val = codegen::Value{agg_info.type, raw_initial};
        storage_.SetValueSkipNull(codegen, space, agg_info.storage_index,
                                  initial_val);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // The initial value for COUNT(*) is 1
        auto one = codegen::Value{type::BigInt::Instance(), codegen.Const64(1)};
        storage_.SetValueSkipNull(codegen, space, agg_info.storage_index, one);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // AVG() aggregates aren't physically stored
        // skip distinct check and continue in loop
        continue;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when creating initial values",
            ExpressionTypeToString(agg_info.aggregate_type).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, message};
      }
    }

    // Add key to hash table if distinct and NOT AVG (there is no hash table for
    // AVG)
    if (agg_info.is_distinct) {
      auto &hash_table = hash_table_infos_[agg_info.hast_table_index].first;
      auto hash_table_id = hash_table_infos_[agg_info.hast_table_index].second;

      // Get runtime state pointer for this hash table
      llvm::Value *state_pointer =
          runtime_state.LoadStatePtr(codegen, hash_table_id);

      // TODO: is it possible to cache the hash value for this expression?
      llvm::Value *hash = nullptr;

      // Prepare the hash keys
      std::vector<codegen::Value> key = grouping_keys;
      key.push_back(input_val);

      // Perform the lookup in the hash table
      hash_table.Insert(codegen, state_pointer, hash, key);
    }
  }

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
}

void Aggregation::DoAdvanceValue(CodeGen &codegen, llvm::Value *space,
                                 const Aggregation::AggregateInfo &agg_info,
                                 const codegen::Value &update) const {
  codegen::Value next;
  switch (agg_info.aggregate_type) {
    case ExpressionType::AGGREGATE_SUM: {
      // Simply increment the current aggregate value by the provided update
      auto curr =
          storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);
      next = curr.Add(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_MIN: {
      // Determine the minimum of the current aggregate value and the update
      auto curr =
          storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);
      next = curr.Min(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_MAX: {
      // Determine the maximum of the current aggregate value and the update
      auto curr =
          storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);
      next = curr.Max(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_COUNT: {
      auto curr =
          storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);

      // Convert the next update into 0 or 1 depending of if it is NULL
      codegen::Value raw_update;
      if (update.IsNullable()) {
        llvm::Value *not_null = update.IsNotNull(codegen);
        raw_update = codegen::Value{
            agg_info.type, codegen->CreateZExt(not_null, codegen.Int64Type())};
      } else {
        raw_update = codegen::Value{agg_info.type, codegen.Const64(1)};
      }

      // Add to aggregate
      next = curr.Add(codegen, raw_update);
      break;
    }
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      // Simply increment current count by one
      auto curr =
          storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);
      auto delta = Value{type::BigInt::Instance(), codegen.Const64(1)};
      next = curr.Add(codegen, delta);
      break;
    }
    case ExpressionType::AGGREGATE_AVG: {
      // AVG() aggregates aren't physically stored
      break;
    }
    default: {
      std::string message = StringUtil::Format(
          "Unexpected aggregate type [%s] when advancing aggregator",
          ExpressionTypeToString(agg_info.aggregate_type).c_str());
      LOG_ERROR("%s", message.c_str());
      throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, message};
    }
  }

  // Store the updated value in the appropriate slot
  PL_ASSERT(next.GetType().type_id != peloton::type::TypeId::INVALID);
  storage_.SetValueSkipNull(codegen, space, agg_info.storage_index, next);
}

void Aggregation::DoNullCheck(CodeGen &codegen, llvm::Value *space,
                              const Aggregation::AggregateInfo &aggregate_info,
                              const codegen::Value &update,
                              UpdateableStorage::NullBitmap &null_bitmap,
                              llvm::Value *curr_val) const {
  // This aggregate is NULL-able, we need to check if the update value is
  // NULL, and whether the current value of the aggregate is NULL.

  // There are two cases we handle:
  // (1). If neither the update value or the current aggregate value are
  //      NULL, we simple do the regular aggregation without NULL checking.
  // (2). If the update value is not NULL, but the current aggregate **is**
  //      NULL, then we just store this update value as if we're creating it
  //      for the first time.
  //
  // If either the update value or the current aggregate are NULL, we have
  // nothing to do.

  llvm::Value *update_not_null = update.IsNotNull(codegen);
  llvm::Value *agg_null =
      null_bitmap.IsNull(codegen, aggregate_info.storage_index);

  if (curr_val == nullptr) {
    curr_val = null_bitmap.ByteFor(codegen, aggregate_info.storage_index);
  }

  lang::If valid_update{
      codegen, update_not_null,
      "Agg" + std::to_string(aggregate_info.source_index) + ".IfValidUpdate"};
  {
    lang::If agg_is_null{
        codegen, agg_null,
        "Agg" + std::to_string(aggregate_info.source_index) + ".IfAggIsNull"};
    {
      // (2)
      switch (aggregate_info.aggregate_type) {
        case ExpressionType::AGGREGATE_SUM:
        case ExpressionType::AGGREGATE_MIN:
        case ExpressionType::AGGREGATE_MAX: {
          storage_.SetValue(codegen, space, aggregate_info.storage_index,
                            update, null_bitmap);
          break;
        }
        case ExpressionType::AGGREGATE_COUNT: {
          codegen::Value one{type::BigInt::Instance(), codegen.Const64(1)};
          storage_.SetValue(codegen, space, aggregate_info.storage_index, one,
                            null_bitmap);
          break;
        }
        default: { break; }
      }
    }
    agg_is_null.ElseBlock("Agg" + std::to_string(aggregate_info.source_index) +
                          ".IfAggIsNotNull");
    {
      // (1)
      DoAdvanceValue(codegen, space, aggregate_info, update);
    }
    agg_is_null.EndIf();

    // Merge the null value
    null_bitmap.MergeValues(agg_is_null, curr_val);
  }
  valid_update.EndIf();

  // Merge the null value
  null_bitmap.MergeValues(valid_update, curr_val);
}

// Advance each of the aggregates stored in the provided storage space
void Aggregation::AdvanceValues(
    CompilationContext &context, llvm::Value *space,
    const std::vector<codegen::Value> &next_vals,
    const std::vector<codegen::Value> &grouping_keys) const {
  auto &codegen = context.GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();

  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, space};

  // Loop over all aggregates, advancing each
  for (const auto &aggregate_info : aggregate_infos_) {
    // AVG() aggregates physically split into SUM() and COUNT(). Hence, they are
    // logical and never directly updated.
    if (aggregate_info.aggregate_type == ExpressionType::AGGREGATE_AVG) {
      continue;
    }

    const Value &update = next_vals[aggregate_info.source_index];

    // Check if aggregation is distinct, then add another hash table lookup
    // before
    // advancing the value
    if (aggregate_info.is_distinct) {
      auto &hash_table =
          hash_table_infos_[aggregate_info.hast_table_index].first;
      auto hash_table_id =
          hash_table_infos_[aggregate_info.hast_table_index].second;

      // Get runtime state pointer for this hash table
      llvm::Value *state_pointer =
          runtime_state.LoadStatePtr(codegen, hash_table_id);

      // TODO: is it possible to cache the hash value for this expression?
      llvm::Value *hash = nullptr;

      // Prepare the hash keys
      // if not global, start with grouping keys, then add aggregation key
      std::vector<codegen::Value> key;
      if (!IsGlobal()) {
        key = grouping_keys;
      }
      key.push_back(update);

      // Perform the lookup in the hash table
      OAHashTable::ProbeResult probe_result =
          hash_table.ProbeOrInsert(codegen, state_pointer, hash, key);

      // Prepare condition for If
      llvm::Value *condition = codegen->CreateNot(probe_result.key_exists);

      // Create llvm value for bitmap byte here already, so we can create the
      // Phi
      // to merge the two paths that are created by the hast table
      llvm::Value *curr_val =
          null_bitmap.ByteFor(codegen, aggregate_info.storage_index);

      // Only process aggregation if value is distinct (key didn't exist in hash
      // table)
      lang::If agg_is_distinct{codegen, condition,
                               "Agg" +
                                   std::to_string(aggregate_info.source_index) +
                                   ".AdvanceValues.IfAggValueIsDistinct"};
      {
        // If the aggregate is not NULL-able, avoid NULL checking altogether and
        // generate the fast-path route.
        if (!null_bitmap.IsNullable(aggregate_info.storage_index)) {
          DoAdvanceValue(codegen, space, aggregate_info, update);
        } else {
          // else do NULL check, which will then call DoAdvanceValue if
          // appropriate
          DoNullCheck(codegen, space, aggregate_info, update, null_bitmap,
                      curr_val);
        }
      }
      agg_is_distinct.EndIf();

      // A hash table lookup also produces several paths, so we need to merge
      // the NullBitmap values
      null_bitmap.MergeValues(agg_is_distinct, curr_val);
    } else {
      // Aggregation is not distinct, just advance value

      // If the aggregate is not NULL-able, avoid NULL checking altogether and
      // generate the fast-path route.
      if (!null_bitmap.IsNullable(aggregate_info.storage_index)) {
        DoAdvanceValue(codegen, space, aggregate_info, update);
        continue;
      }

      // do NULL check, which will call DoAdvanceValue if appropriate
      DoNullCheck(codegen, space, aggregate_info, update, null_bitmap);
    }
  }

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
}

// Advance all stored aggregates (stored in the provided storage space) using
// the values in the provided vector// Helper Function to advance global
// aggregations
// that creates an empty vector for the reference
void Aggregation::AdvanceValues(CompilationContext &context, llvm::Value *space,
                                const std::vector<codegen::Value> &next) const {
  // Create empty vector and hand the reference to the actual implementation
  // makes it easier to call this function without providing grouping keys
  std::vector<codegen::Value> empty;
  AdvanceValues(context, space, next, empty);
}

// This function will computes the final values of all aggregates stored in the
// provided storage space, and populates the provided vector with these values.
void Aggregation::FinalizeValues(
    CodeGen &codegen, llvm::Value *space,
    std::vector<codegen::Value> &final_vals) const {
  // Collect all final values into this map. We need this because some
  // aggregates are derived from other component aggregates.
  // bool shows if distinct
  std::map<std::tuple<uint32_t, ExpressionType, bool>, codegen::Value> vals;

  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, space};

  for (const auto &agg_info : aggregate_infos_) {
    uint32_t source = agg_info.source_index;
    ExpressionType agg_type = agg_info.aggregate_type;
    switch (agg_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        codegen::Value final_val;
        if (null_bitmap.IsNullable(agg_info.storage_index)) {
          final_val = storage_.GetValue(codegen, space, agg_info.storage_index,
                                        null_bitmap);
        } else {
          final_val =
              storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);
        }

        vals[std::make_tuple(source, agg_type, agg_info.is_distinct)] =
            final_val;
        if (!agg_info.is_internal) {
          final_vals.push_back(final_val);
        }
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // Find the sum and count for this aggregate
        auto count_key = std::make_tuple(
            source, ExpressionType::AGGREGATE_COUNT, agg_info.is_distinct);
        auto sum_key = std::make_tuple(source, ExpressionType::AGGREGATE_SUM,
                                       agg_info.is_distinct);
        PL_ASSERT(vals.find(count_key) != vals.end());
        PL_ASSERT(vals.find(sum_key) != vals.end());

        codegen::Value count =
            vals[count_key].CastTo(codegen, type::Decimal::Instance());

        codegen::Value sum =
            vals[sum_key].CastTo(codegen, type::Decimal::Instance());

        codegen::Value final_val = sum.Div(codegen, count, OnError::ReturnNull);

        vals[std::make_tuple(source, agg_type, agg_info.is_distinct)] =
            final_val;
        final_vals.push_back(final_val);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Neither COUNT(...) or COUNT(*) can ever return NULL, so no NULL-check
        codegen::Value final_val =
            storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);

        vals[std::make_tuple(source, agg_type, agg_info.is_distinct)] =
            final_val;
        if (!agg_info.is_internal) {
          final_vals.push_back(final_val);
        }
        break;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when finalizing aggregator",
            ExpressionTypeToString(agg_type).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, message};
      }
    }
  }
}

}  // namespace codegen
}  // namespace peloton
