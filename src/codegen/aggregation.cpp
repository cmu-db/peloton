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
#include "common/logger.h"

namespace peloton {
namespace codegen {

// TODO: Handle shared/duplicated aggregates

// Configure/setup the aggregation class to handle the provided aggregate types
void Aggregation::Setup(
    CodeGen &codegen,
    const std::vector<planner::AggregatePlan::AggTerm> &aggregates,
    bool is_global) {
  is_global_ = is_global;

  for (uint32_t source_idx = 0; source_idx < aggregates.size(); source_idx++) {
    const auto &agg_term = aggregates[source_idx];
    switch (agg_term.aggtype) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Add the count to the storage layout. COUNTs are never NULL-able.
        const type::Type count_type{type::BigInt::Instance()};
        uint32_t storage_pos = storage_.AddType(count_type);

        // Add metadata for the count aggregate
        AggregateInfo aggregate_info{agg_term.aggtype, count_type, source_idx,
                                     storage_pos, false};
        aggregate_infos_.push_back(aggregate_info);
        break;
      }
      case ExpressionType::AGGREGATE_SUM:
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
        AggregateInfo aggregate_info{agg_term.aggtype, value_type, source_idx,
                                     storage_pos, false};
        aggregate_infos_.push_back(aggregate_info);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // We decompose averages into separate COUNT() and SUM() components

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
                              source_idx, sum_storage_pos, true};
        aggregate_infos_.push_back(sum_agg);

        // COUNT() - can use big integer since we're counting instances
        // First add the count to the storage layout
        uint32_t count_storage_pos = storage_.AddType(type::BigInt::Instance());

        // Add metadata about the COUNT() aggregate
        AggregateInfo count_agg{ExpressionType::AGGREGATE_COUNT,
                                type::BigInt::Instance(), source_idx,
                                count_storage_pos, true};
        aggregate_infos_.push_back(count_agg);

        // AVG() - this isn't storage physically, but we need metadata about it
        AggregateInfo avg_agg{agg_term.aggtype, type::Decimal::Instance(),
                              source_idx, source_idx, false};
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
    const std::vector<codegen::Value> &initial) const {
  // Global aggregations should be calling CreateInitialGlobalValues(...)
  PL_ASSERT(!IsGlobal());

  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, space};

  // Initialize bitmap to all NULLs
  null_bitmap.InitAllNull(codegen);

  for (uint32_t i = 0; i < aggregate_infos_.size(); i++) {
    const auto &agg_info = aggregate_infos_[i];
    switch (agg_info.aggregate_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        // For the above aggregations, the initial value is the attribute value
        const auto &input_val = initial[agg_info.source_index];
        if (null_bitmap.IsNullable(i)) {
          storage_.SetValue(codegen, space, agg_info.storage_index, input_val,
                            null_bitmap);
        } else {
          storage_.SetValueSkipNull(codegen, space, agg_info.storage_index,
                                    input_val);
        }
        break;
      }
      case ExpressionType::AGGREGATE_COUNT: {
        const auto &input_val = initial[agg_info.source_index];
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
        break;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when creating initial values",
            ExpressionTypeToString(agg_info.aggregate_type).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, message};
      }
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
      return;
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

// Advance each of the aggregates stored in the provided storage space
void Aggregation::AdvanceValues(
    CodeGen &codegen, llvm::Value *space,
    const std::vector<codegen::Value> &next_vals) const {
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

    // If the aggregate is not NULL-able, avoid NULL checking altogether and
    // generate the fast-path route.
    if (!null_bitmap.IsNullable(aggregate_info.storage_index)) {
      DoAdvanceValue(codegen, space, aggregate_info, update);
      continue;
    }

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

    llvm::Value *curr_val =
        null_bitmap.ByteFor(codegen, aggregate_info.storage_index);

    lang::If valid_update{codegen, update_not_null};
    {
      lang::If agg_is_null{codegen, agg_null};
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
      agg_is_null.ElseBlock();
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

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
}

// This function will computes the final values of all aggregates stored in the
// provided storage space, and populates the provided vector with these values.
void Aggregation::FinalizeValues(
    CodeGen &codegen, llvm::Value *space,
    std::vector<codegen::Value> &final_vals) const {
  // Collect all final values into this map. We need this because some
  // aggregates are derived from other component aggregates.
  std::map<std::pair<uint32_t, ExpressionType>, codegen::Value> vals;

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

        vals[std::make_pair(source, agg_type)] = final_val;
        if (!agg_info.is_internal) {
          final_vals.push_back(final_val);
        }
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // Find the sum and count for this aggregate
        auto count_key =
            std::make_pair(source, ExpressionType::AGGREGATE_COUNT);
        auto sum_key = std::make_pair(source, ExpressionType::AGGREGATE_SUM);
        PL_ASSERT(vals.find(count_key) != vals.end());
        PL_ASSERT(vals.find(sum_key) != vals.end());

        codegen::Value count =
            vals[count_key].CastTo(codegen, type::Decimal::Instance());

        codegen::Value sum =
            vals[sum_key].CastTo(codegen, type::Decimal::Instance());

        codegen::Value final_val = sum.Div(codegen, count, OnError::ReturnNull);

        vals[std::make_pair(source, agg_type)] = final_val;
        final_vals.push_back(final_val);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Neither COUNT(...) or COUNT(*) can ever return NULL, so no NULL-check
        codegen::Value final_val =
            storage_.GetValueSkipNull(codegen, space, agg_info.storage_index);

        vals[std::make_pair(source, agg_type)] = final_val;
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
