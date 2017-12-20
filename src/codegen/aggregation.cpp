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

#include "codegen/proxy/oa_hash_table_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/bigint_type.h"
#include "codegen/type/decimal_type.h"

namespace peloton {
namespace codegen {

// Configure/setup the aggregation class to handle the provided aggregate types
void Aggregation::Setup(
    CodeGen &codegen,
    const std::vector<planner::AggregatePlan::AggTerm> &aggregates,
    bool is_global, std::vector<type::Type> &grouping_ai_types) {
  is_global_ = is_global;

  for (uint32_t source_idx = 0; source_idx < aggregates.size(); source_idx++) {
    const auto &agg_term = aggregates[source_idx];
    switch (agg_term.aggtype) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Add the count to the storage layout. COUNTs are never NULL-able.
        const type::Type count_type{type::BigInt::Instance()};
        uint32_t storage_pos = storage_.AddType(count_type);

        // Add metadata for the aggregate
        AggregateInfo agg_info{agg_term.aggtype,
                               source_idx,
                               {{storage_pos}},
                               agg_term.distinct,
                               0};
        aggregate_infos_.push_back(agg_info);
        break;
      }
      case ExpressionType::AGGREGATE_SUM: {
        // Add the element to the storage layout
        auto value_type = agg_term.expression->ResultType();

        // If we're doing a global aggregation, the aggregate can potentially be
        // NULL (i.e., if there are no rows in the source table).
        if (IsGlobal()) {
          value_type = value_type.AsNullable();
        }
        uint32_t storage_pos = storage_.AddType(value_type);

        // Add metadata for the aggregate
        AggregateInfo agg_info{agg_term.aggtype,
                               source_idx,
                               {{storage_pos}},
                               agg_term.distinct,
                               0};
        aggregate_infos_.push_back(agg_info);
        break;
      }
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        // Add the element to the storage layout
        auto value_type = agg_term.expression->ResultType();

        // If we're doing a global aggregation, the aggregate can potentially be
        // NULL (i.e., if there are no rows in the source table).
        if (IsGlobal()) {
          value_type = value_type.AsNullable();
        }
        uint32_t storage_pos = storage_.AddType(value_type);

        // Add metadata for the aggregate
        AggregateInfo agg_info{agg_term.aggtype,
                               source_idx,
                               {{storage_pos}},
                               agg_term.distinct,
                               0};
        aggregate_infos_.push_back(agg_info);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // We decompose averages into separate SUM() and COUNT() components

        // SUM() - the type must match the type of the expression
        PL_ASSERT(agg_term.expression != nullptr);
        auto sum_type = agg_term.expression->ResultType();
        if (IsGlobal()) {
          sum_type = sum_type.AsNullable();
        }
        uint32_t sum_storage_pos = storage_.AddType(sum_type);

        // COUNT() - can use big integer since we're counting instances
        uint32_t count_storage_pos = storage_.AddType(type::BigInt::Instance());

        // Add metadata for the aggregate
        AggregateInfo agg_info{agg_term.aggtype,
                               source_idx,
                               {{sum_storage_pos, count_storage_pos}},
                               agg_term.distinct,
                               0};
        aggregate_infos_.push_back(agg_info);
        break;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when preparing aggregator",
            ExpressionTypeToString(agg_term.aggtype).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{ExceptionType::UNKNOWN_TYPE, message};
      }
    }
  }

  // Create hash tables for distinct aggregates
  for (auto &agg_info : aggregate_infos_) {
    if (agg_info.aggregate_type == ExpressionType::AGGREGATE_MIN ||
        agg_info.aggregate_type == ExpressionType::AGGREGATE_MAX) {
      // It doesn't pay off for MIN/MAX to use a hash table for distinct values,
      // since applying the MIM/MAX instruction every time is cheaper - so skip
      // the distinct part
      agg_info.is_distinct = false;
      continue;
    }

    if (!agg_info.is_distinct) {
      // No hash tables needed for non-distinct aggregates
      continue;
    }

    // Register the hash-table instance in the runtime state
    auto hash_table_id = runtime_state_.RegisterState(
        "agg_hash" + std::to_string(agg_info.source_index),
        OAHashTableProxy::GetType(codegen));

    // Prepare the hash keys
    // if not global, start with grouping keys, then add aggregation key
    std::vector<type::Type> key_type;
    if (!IsGlobal()) {
      key_type = grouping_ai_types;
    }
    key_type.push_back(
        aggregates[agg_info.source_index].expression->ResultType());

    // Create the hash table. We don't need to save any values, hence the zero
    // payload value size.
    auto hash_table = OAHashTable{codegen, key_type, 0};

    hash_table_infos_.emplace_back(std::move(hash_table), hash_table_id);

    // Add index to the aggregation_info struct
    agg_info.hast_table_index =
        static_cast<uint32_t>(hash_table_infos_.size() - 1);
  }

  // Finalize the storage format
  storage_.Finalize(codegen);
}

// Setup the aggregation to handle the provided aggregates
void Aggregation::Setup(
    CodeGen &codegen,
    const std::vector<planner::AggregatePlan::AggTerm> &agg_terms,
    bool is_global) {
  PL_ASSERT(is_global);
  // Create empty vector and hand the reference to the actual implementation
  // makes it easier to call this function without providing grouping keys
  std::vector<type::Type> empty;
  Setup(codegen, agg_terms, is_global, empty);
}

// Codegen any initialization work for the hash tables
void Aggregation::InitializeState(CodeGen &codegen) {
  for (auto hash_table_info : hash_table_infos_) {
    auto &hash_table = hash_table_info.first;
    auto hash_table_id = hash_table_info.second;
    hash_table.Init(codegen,
                    runtime_state_.LoadStatePtr(codegen, hash_table_id));
  }
}

// Cleanup by destroying the aggregation hash tables
void Aggregation::TearDownState(CodeGen &codegen) {
  for (auto hash_table_info : hash_table_infos_) {
    auto &hash_table = hash_table_info.first;
    auto hash_table_id = hash_table_info.second;
    hash_table.Destroy(codegen,
                       runtime_state_.LoadStatePtr(codegen, hash_table_id));
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
    CodeGen &codegen, llvm::Value *space,
    const std::vector<codegen::Value> &initial,
    const std::vector<codegen::Value> &grouping_keys) const {
  // Global aggregations should be calling CreateInitialGlobalValues(...)
  PL_ASSERT(!IsGlobal());

  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, space};

  // Initialize bitmap to all NULLs
  null_bitmap.InitAllNull(codegen);

  // Iterate over the aggregations
  for (uint32_t i = 0; i < aggregate_infos_.size(); i++) {
    const auto &agg_info = aggregate_infos_[i];
    const auto &input_val = initial[agg_info.source_index];

    switch (agg_info.aggregate_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX:
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Those aggregations consist of only one component
        DoInitializeValue(codegen, space, agg_info.aggregate_type,
                          agg_info.storage_indices[0], input_val, null_bitmap);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // AVG has to initialize both the SUM and the COUNT
        DoInitializeValue(codegen, space, ExpressionType::AGGREGATE_SUM,
                          agg_info.storage_indices[0], input_val, null_bitmap);
        DoInitializeValue(codegen, space, ExpressionType::AGGREGATE_COUNT,
                          agg_info.storage_indices[1], input_val, null_bitmap);
        break;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when creating initial values",
            ExpressionTypeToString(agg_info.aggregate_type).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{ExceptionType::UNKNOWN_TYPE, message};
      }
    }

    // Write the final contents of the null bitmap
    null_bitmap.WriteBack(codegen);

    // Add key to hash table if distinct
    if (agg_info.is_distinct) {
      auto &hash_table = hash_table_infos_[agg_info.hast_table_index].first;
      auto hash_table_id = hash_table_infos_[agg_info.hast_table_index].second;

      // Get runtime state pointer for this hash table
      llvm::Value *state_pointer =
          runtime_state_.LoadStatePtr(codegen, hash_table_id);

      // TODO(marcel): is it possible to cache the hash value for this
      // expression?
      llvm::Value *hash = nullptr;

      // Prepare the hash keys
      std::vector<codegen::Value> key = grouping_keys;
      key.push_back(input_val);

      // Perform the dummy lookup in the hash table that creates the entry
      HashTable::NoOpInsertCallback insert_callback;
      hash_table.Insert(codegen, state_pointer, hash, key, insert_callback);
    }
  }  // iterate aggregations
}

void Aggregation::DoInitializeValue(
    CodeGen &codegen, llvm::Value *space, ExpressionType type,
    uint32_t storage_index, const Value &initial,
    UpdateableStorage::NullBitmap &null_bitmap) const {
  switch (type) {
    case ExpressionType::AGGREGATE_SUM:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_MAX: {
      // For the above aggregations, the initial value is the attribute value
      if (null_bitmap.IsNullable(storage_index)) {
        storage_.SetValue(codegen, space, storage_index, initial, null_bitmap);
      } else {
        storage_.SetValueSkipNull(codegen, space, storage_index, initial);
      }
      break;
    }
    case ExpressionType::AGGREGATE_COUNT: {
      llvm::Value *raw_initial = nullptr;
      if (initial.IsNullable()) {
        llvm::Value *not_null = initial.IsNotNull(codegen);
        raw_initial = codegen->CreateZExt(not_null, codegen.Int64Type());
      } else {
        raw_initial = codegen.Const64(1);
      }
      auto initial_val = codegen::Value{type::BigInt::Instance(), raw_initial};
      storage_.SetValueSkipNull(codegen, space, storage_index, initial_val);
      break;
    }
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      // The initial value for COUNT(*) is 1
      auto one = codegen::Value{type::BigInt::Instance(), codegen.Const64(1)};
      storage_.SetValueSkipNull(codegen, space, storage_index, one);
      break;
    }
    default: {
      std::string message = StringUtil::Format(
          "Unexpected aggregate type [%s] when creating initial values",
          ExpressionTypeToString(type).c_str());
      LOG_ERROR("%s", message.c_str());
      throw Exception{ExceptionType::UNKNOWN_TYPE, message};
    }
  }
}

void Aggregation::DoAdvanceValue(CodeGen &codegen, llvm::Value *space,
                                 ExpressionType type, uint32_t storage_index,
                                 const codegen::Value &update) const {
  codegen::Value next;
  switch (type) {
    case ExpressionType::AGGREGATE_SUM: {
      auto curr = storage_.GetValueSkipNull(codegen, space, storage_index);
      next = curr.Add(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_MIN: {
      auto curr = storage_.GetValueSkipNull(codegen, space, storage_index);
      next = curr.Min(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_MAX: {
      auto curr = storage_.GetValueSkipNull(codegen, space, storage_index);
      next = curr.Max(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_COUNT: {
      auto curr = storage_.GetValueSkipNull(codegen, space, storage_index);

      // Convert the next update into 0 or 1 depending of if it is NULL
      codegen::Value raw_update;
      if (update.IsNullable()) {
        llvm::Value *not_null = update.IsNotNull(codegen);
        raw_update =
            codegen::Value{type::BigInt::Instance(),
                           codegen->CreateZExt(not_null, codegen.Int64Type())};
      } else {
        raw_update =
            codegen::Value{type::BigInt::Instance(), codegen.Const64(1)};
      }

      // Add to aggregate
      next = curr.Add(codegen, raw_update);
      break;
    }
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      auto curr = storage_.GetValueSkipNull(codegen, space, storage_index);
      auto delta = Value{type::BigInt::Instance(), codegen.Const64(1)};
      next = curr.Add(codegen, delta);
      break;
    }
    default: {
      std::string message = StringUtil::Format(
          "Unexpected aggregate type [%s] when advancing aggregator",
          ExpressionTypeToString(type).c_str());
      LOG_ERROR("%s", message.c_str());
      throw Exception{ExceptionType::UNKNOWN_TYPE, message};
    }
  }

  // Store the updated value in the appropriate slot
  PL_ASSERT(next.GetType().type_id != peloton::type::TypeId::INVALID);
  storage_.SetValueSkipNull(codegen, space, storage_index, next);
}

void Aggregation::DoNullCheck(
    CodeGen &codegen, llvm::Value *space, ExpressionType type,
    uint32_t storage_index, const codegen::Value &update,
    UpdateableStorage::NullBitmap &null_bitmap) const {
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
  llvm::Value *agg_null = null_bitmap.IsNull(codegen, storage_index);

  // Fetch null byte so we can phi-resolve it after all the branches
  llvm::Value *null_byte_snapshot = null_bitmap.ByteFor(codegen, storage_index);

  lang::If valid_update{codegen, update_not_null, "Agg.IfValidUpdate"};
  {
    lang::If agg_is_null{codegen, agg_null, "Agg.IfAggIsNull"};
    {
      // (2)
      switch (type) {
        case ExpressionType::AGGREGATE_SUM:
        case ExpressionType::AGGREGATE_MIN:
        case ExpressionType::AGGREGATE_MAX: {
          storage_.SetValue(codegen, space, storage_index, update, null_bitmap);
          break;
        }
        case ExpressionType::AGGREGATE_COUNT: {
          codegen::Value one{type::BigInt::Instance(), codegen.Const64(1)};
          storage_.SetValue(codegen, space, storage_index, one, null_bitmap);
          break;
        }
        default: { break; }
      }
    }
    agg_is_null.ElseBlock("Agg.IfAggIsNotNull");
    {
      // (1)
      DoAdvanceValue(codegen, space, type, storage_index, update);
    }
    agg_is_null.EndIf();

    // Merge the null value
    null_bitmap.MergeValues(agg_is_null, null_byte_snapshot);
  }
  valid_update.EndIf();

  // Merge the null value
  null_bitmap.MergeValues(valid_update, null_byte_snapshot);
}

// Advance the value of a specific aggregate. Performs NULL check if necessary
// and finally calls DoAdvanceValue().
void Aggregation::AdvanceValue(
    CodeGen &codegen, llvm::Value *space,
    const std::vector<codegen::Value> &next_vals,
    const Aggregation::AggregateInfo &aggregate_info,
    UpdateableStorage::NullBitmap &null_bitmap) const {
  const Value &update = next_vals[aggregate_info.source_index];

  switch (aggregate_info.aggregate_type) {
    case ExpressionType::AGGREGATE_SUM:
    case ExpressionType::AGGREGATE_MIN:
    case ExpressionType::AGGREGATE_MAX: {
      // If the aggregate is not NULL-able, elide NULL check
      if (!null_bitmap.IsNullable(aggregate_info.storage_indices[0])) {
        DoAdvanceValue(codegen, space, aggregate_info.aggregate_type,
                       aggregate_info.storage_indices[0], update);
      } else {
        DoNullCheck(codegen, space, aggregate_info.aggregate_type,
                    aggregate_info.storage_indices[0], update, null_bitmap);
      }
      break;
    }
    case ExpressionType::AGGREGATE_COUNT:
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      // COUNT can't be nullable, so skip the null check
      DoAdvanceValue(codegen, space, ExpressionType::AGGREGATE_COUNT,
                     aggregate_info.storage_indices[0], update);
      break;
    }
    case ExpressionType::AGGREGATE_AVG: {
      // If the SUM is not NULL-able, elide NULL check
      if (!null_bitmap.IsNullable(aggregate_info.storage_indices[0])) {
        DoAdvanceValue(codegen, space, ExpressionType::AGGREGATE_SUM,
                       aggregate_info.storage_indices[0], update);
      } else {
        DoNullCheck(codegen, space, ExpressionType::AGGREGATE_SUM,
                    aggregate_info.storage_indices[0], update, null_bitmap);
      }

      // COUNT can't be nullable, so skip the null check
      DoAdvanceValue(codegen, space, ExpressionType::AGGREGATE_COUNT,
                     aggregate_info.storage_indices[1], update);

      break;
    }
    default: {
      std::string message = StringUtil::Format(
          "Unexpected aggregate type [%s] when advancing aggregator",
          ExpressionTypeToString(aggregate_info.aggregate_type).c_str());
      LOG_ERROR("%s", message.c_str());
      throw Exception{ExceptionType::UNKNOWN_TYPE, message};
    }
  }  // switch
}

// Advance each of the aggregates stored in the provided storage space
void Aggregation::AdvanceValues(
    CodeGen &codegen, llvm::Value *space,
    const std::vector<codegen::Value> &next_vals,
    const std::vector<codegen::Value> &grouping_keys) const {
  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, space};

  // Loop over all aggregates, advancing each
  for (const auto &aggregate_info : aggregate_infos_) {
    const Value &update = next_vals[aggregate_info.source_index];

    if (!aggregate_info.is_distinct) {
      // Aggregation is not distinct, just advance value
      AdvanceValue(codegen, space, next_vals, aggregate_info, null_bitmap);
      continue;
    }

    // Check if aggregation is distinct, then add another hash table lookup
    // before advancing the value
    auto &hash_table = hash_table_infos_[aggregate_info.hast_table_index].first;
    auto hash_table_id =
        hash_table_infos_[aggregate_info.hast_table_index].second;

    // Get runtime state pointer for this hash table
    llvm::Value *ht_ptr = runtime_state_.LoadStatePtr(codegen, hash_table_id);

    // TODO(marcel): is it possible to cache the hash value for this
    // expression?
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
        hash_table.ProbeOrInsert(codegen, ht_ptr, hash, key);

    // Prepare condition for If
    llvm::Value *condition = codegen->CreateNot(probe_result.key_exists);

    // Grab a snapshot of the NULL indicator before the branch because it may
    // change after inside the if-clause. After the if-clause, we can use the
    // snapshot before and after to correctly resolve the final NULL indication
    // bit.
    llvm::Value *null_byte_snapshot =
        null_bitmap.ByteFor(codegen, aggregate_info.storage_indices[0]);

    // Only process aggregation if value is distinct (key didn't exist in hash
    // table)
    auto name = "agg" + std::to_string(aggregate_info.source_index) +
                ".advanceValues.ifAggValueIsDistinct";
    lang::If agg_is_distinct{codegen, condition, name};
    {
      // Advance value
      AdvanceValue(codegen, space, next_vals, aggregate_info, null_bitmap);
    }
    agg_is_distinct.EndIf();

    // Merge the NULL indicator (potentially modified in the branch) with the
    // snapshot we grabbed before the if-clause to resolve the final value.
    null_bitmap.MergeValues(agg_is_distinct, null_byte_snapshot);
  }

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
}

// Advance all stored aggregates (stored in the provided storage space) using
// the update values provided in "next". Since global aggregations don't
// have grouping keys, this function creates an empty vector of keys in order
// to use AdvanceValues() that assumes grouping keys.
void Aggregation::AdvanceValues(CodeGen &codegen, llvm::Value *space,
                                const std::vector<codegen::Value> &next) const {
  std::vector<codegen::Value> empty;
  AdvanceValues(codegen, space, next, empty);
}

// This function will compute the final values of all aggregates stored in the
// provided storage space, populating the provided vector with these values.
void Aggregation::FinalizeValues(
    CodeGen &codegen, llvm::Value *space,
    std::vector<codegen::Value> &final_vals) const {
  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, space};

  for (const auto &agg_info : aggregate_infos_) {
    ExpressionType agg_type = agg_info.aggregate_type;
    switch (agg_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        codegen::Value final_val;
        if (null_bitmap.IsNullable(agg_info.storage_indices[0])) {
          final_val = storage_.GetValue(
              codegen, space, agg_info.storage_indices[0], null_bitmap);
        } else {
          final_val = storage_.GetValueSkipNull(codegen, space,
                                                agg_info.storage_indices[0]);
        }

        // append final value to result vector
        final_vals.push_back(final_val);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // collect the final values of the SUM and the COUNT components and
        // calculate the average
        codegen::Value sum;
        if (null_bitmap.IsNullable(agg_info.storage_indices[0])) {
          sum = storage_.GetValue(codegen, space, agg_info.storage_indices[0],
                                  null_bitmap);
        } else {
          sum = storage_.GetValueSkipNull(codegen, space,
                                          agg_info.storage_indices[0]);
        }

        codegen::Value count = storage_.GetValueSkipNull(
            codegen, space, agg_info.storage_indices[1]);

        // cast the values to DECIMAL
        codegen::Value sum_casted =
            sum.CastTo(codegen, type::Decimal::Instance());
        codegen::Value count_casted =
            count.CastTo(codegen, type::Decimal::Instance());

        // add the actual calculation
        codegen::Value final_val =
            sum_casted.Div(codegen, count_casted, OnError::ReturnNull);

        // append final value to result vector
        final_vals.push_back(final_val);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Neither COUNT(...) or COUNT(*) can ever return NULL, so no NULL-check
        codegen::Value final_val = storage_.GetValueSkipNull(
            codegen, space, agg_info.storage_indices[0]);

        // append final value to result vector
        final_vals.push_back(final_val);
        break;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when finalizing aggregator",
            ExpressionTypeToString(agg_type).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{ExceptionType::UNKNOWN_TYPE, message};
      }
    }
  }
}

}  // namespace codegen
}  // namespace peloton
